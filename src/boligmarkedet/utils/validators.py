"""Data validation utilities."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

from .logging import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationError:
    """Represents a validation error."""
    field: str
    value: Any
    message: str


@dataclass 
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[ValidationError]
    cleaned_data: Optional[Dict[str, Any]] = None


class PropertyValidator:
    """Validator for property data from Boliga API."""
    
    @staticmethod
    def validate_active_property(data: Dict[str, Any]) -> ValidationResult:
        """Validate active property data.
        
        Args:
            data: Raw property data from API
            
        Returns:
            ValidationResult with validation status and errors
        """
        errors = []
        cleaned_data = {}
        
        # Required fields for active properties
        required_fields = ['id', 'price', 'rooms', 'size', 'city', 'zipCode', 'street']
        
        # Check required fields
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(ValidationError(
                    field=field,
                    value=data.get(field),
                    message=f"Required field '{field}' is missing or null"
                ))
                continue
            
            cleaned_data[field] = data[field]
        
        # Validate specific field types and ranges
        errors.extend(PropertyValidator._validate_common_fields(data, cleaned_data))
        
        # Active property specific validations
        if 'daysForSale' in data:
            days_for_sale = data['daysForSale']
            if isinstance(days_for_sale, (int, float)) and days_for_sale >= 0:
                cleaned_data['daysForSale'] = int(days_for_sale)
            elif days_for_sale is not None:
                errors.append(ValidationError(
                    field='daysForSale',
                    value=days_for_sale,
                    message="daysForSale must be a non-negative number"
                ))
        
        # Validate created date
        if 'createdDate' in data:
            try:
                if data['createdDate']:
                    cleaned_data['createdDate'] = datetime.fromisoformat(
                        data['createdDate'].replace('Z', '+00:00')
                    )
            except (ValueError, AttributeError) as e:
                errors.append(ValidationError(
                    field='createdDate',
                    value=data.get('createdDate'),
                    message=f"Invalid date format: {e}"
                ))
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            cleaned_data=cleaned_data if len(errors) == 0 else None
        )
    
    @staticmethod
    def validate_sold_property(data: Dict[str, Any]) -> ValidationResult:
        """Validate sold property data.
        
        Args:
            data: Raw property data from API
            
        Returns:
            ValidationResult with validation status and errors
        """
        errors = []
        cleaned_data = {}
        
        # Required fields for sold properties
        required_fields = ['estateId', 'address', 'zipCode', 'price', 'soldDate', 'city']
        
        # Check required fields
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(ValidationError(
                    field=field,
                    value=data.get(field),
                    message=f"Required field '{field}' is missing or null"
                ))
                continue
            
            cleaned_data[field] = data[field]
        
        # Validate specific field types and ranges
        errors.extend(PropertyValidator._validate_common_fields(data, cleaned_data))
        
        # Sold property specific validations
        if 'sqmPrice' in data:
            sqm_price = data['sqmPrice']
            if isinstance(sqm_price, (int, float)) and sqm_price >= 0:
                cleaned_data['sqmPrice'] = float(sqm_price)
            elif sqm_price is not None:
                errors.append(ValidationError(
                    field='sqmPrice',
                    value=sqm_price,
                    message="sqmPrice must be a non-negative number"
                ))
        
        # Validate sold date
        if 'soldDate' in data:
            try:
                if data['soldDate']:
                    cleaned_data['soldDate'] = datetime.fromisoformat(
                        data['soldDate'].replace('Z', '+00:00')
                    )
            except (ValueError, AttributeError) as e:
                errors.append(ValidationError(
                    field='soldDate',
                    value=data.get('soldDate'),
                    message=f"Invalid date format: {e}"
                ))
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            cleaned_data=cleaned_data if len(errors) == 0 else None
        )
    
    @staticmethod
    def _validate_common_fields(data: Dict[str, Any], cleaned_data: Dict[str, Any]) -> List[ValidationError]:
        """Validate fields common to both active and sold properties."""
        errors = []
        
        # Validate price
        if 'price' in data:
            price = data['price']
            if isinstance(price, (int, float)) and price > 0:
                cleaned_data['price'] = int(price)
            else:
                errors.append(ValidationError(
                    field='price',
                    value=price,
                    message="Price must be a positive number"
                ))
        
        # Validate rooms
        if 'rooms' in data:
            rooms = data['rooms']
            if isinstance(rooms, (int, float)) and rooms >= 0:
                cleaned_data['rooms'] = float(rooms)
            elif rooms is not None:
                errors.append(ValidationError(
                    field='rooms',
                    value=rooms,
                    message="Rooms must be a non-negative number"
                ))
        
        # Validate size
        if 'size' in data:
            size = data['size']
            if isinstance(size, (int, float)) and size > 0:
                cleaned_data['size'] = int(size)
            elif size is not None:
                errors.append(ValidationError(
                    field='size',
                    value=size,
                    message="Size must be a positive number"
                ))
        
        # Validate zip code
        if 'zipCode' in data:
            zip_code = data['zipCode']
            if isinstance(zip_code, int) and 1000 <= zip_code <= 9999:
                cleaned_data['zipCode'] = zip_code
            else:
                errors.append(ValidationError(
                    field='zipCode',
                    value=zip_code,
                    message="zipCode must be a 4-digit number between 1000-9999"
                ))
        
        # Validate build year
        if 'buildYear' in data:
            build_year = data['buildYear']
            if isinstance(build_year, int) and 1800 <= build_year <= datetime.now().year:
                cleaned_data['buildYear'] = build_year
            elif build_year is not None:
                errors.append(ValidationError(
                    field='buildYear',
                    value=build_year,
                    message=f"buildYear must be between 1800 and {datetime.now().year}"
                ))
        
        # Validate coordinates
        for coord_field, coord_range in [('latitude', (-90, 90)), ('longitude', (-180, 180))]:
            if coord_field in data:
                coord_value = data[coord_field]
                if isinstance(coord_value, (int, float)) and coord_range[0] <= coord_value <= coord_range[1]:
                    cleaned_data[coord_field] = float(coord_value)
                elif coord_value is not None:
                    errors.append(ValidationError(
                        field=coord_field,
                        value=coord_value,
                        message=f"{coord_field} must be between {coord_range[0]} and {coord_range[1]}"
                    ))
        
        return errors


def validate_batch_data(
    data_list: List[Dict[str, Any]], 
    validator_func,
    max_errors: int = 100
) -> Dict[str, Any]:
    """Validate a batch of data records.
    
    Args:
        data_list: List of data records to validate
        validator_func: Validation function to use
        max_errors: Maximum number of errors to collect
        
    Returns:
        Dictionary with validation results and statistics
    """
    valid_records = []
    invalid_records = []
    all_errors = []
    
    for i, record in enumerate(data_list):
        result = validator_func(record)
        
        if result.is_valid:
            valid_records.append(result.cleaned_data)
        else:
            invalid_records.append({
                'index': i,
                'record': record,
                'errors': result.errors
            })
            all_errors.extend(result.errors)
            
            if len(all_errors) >= max_errors:
                logger.warning(f"Reached maximum error limit ({max_errors}). Stopping validation.")
                break
    
    return {
        'valid_count': len(valid_records),
        'invalid_count': len(invalid_records),
        'total_count': len(data_list),
        'valid_records': valid_records,
        'invalid_records': invalid_records,
        'errors': all_errors,
        'success_rate': len(valid_records) / len(data_list) if data_list else 0
    } 