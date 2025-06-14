# Boliga API Documentation

## Base URL
`https://api.boliga.dk`

## Endpoints

### Search Properties
`GET /api/v2/search/results`

#### Query Parameters
| Parameter | Type | Description |
|-----------|------|-------------|
| pageSize | int | Results per page (default: 50) |
| searchTab | int | Search tab index |
| sort | string | Sort order (e.g. "daysForSale-a") |
| propertyType | int | Property type filter |
| priceMin/priceMax | int | Price range |
| roomsMin/roomsMax | int | Number of rooms |
| sizeMin/sizeMax | int | Property size in m² |
| buildYearMin/buildYearMax | int | Construction year |
| energyClassMin/energyClassMax | string | Energy rating |
| q | string | Free text search |
| openHouse | boolean | Show only open houses |

#### Response Structure
```json
{
  "meta": {
    "totalCount": int,
    "totalPages": int,
    "pageIndex": int,
    "pageSize": int
  },
  "results": [
    {
      "id": int,
      "price": int,
      "rooms": float,
      "size": int,
      "lotSize": int,
      "buildYear": int,
      "energyClass": string,
      "city": string,
      "zipCode": int,
      "street": string,
      "latitude": float,
      "longitude": float,
      "images": [
        {
          "url": string
        }
      ],
      "daysForSale": int,
      "createdDate": string
    }
  ]
}
```

### Search Sold Properties
`GET /api/v2/sold/search/results`

#### Query Parameters
| Parameter | Type | Description |
|-----------|------|-------------|
| page | int | Page number |
| searchTab | int | Search tab index |
| sort | string | Sort order (e.g. "date-d") |
| propertyType | int | Property type filter |
| salesDateMin | string | Minimum sale date (YYYY) |
| sizeMin/sizeMax | int | Property size in m² |
| roomsMin/roomsMax | int | Number of rooms |
| buildYearMin/buildYearMax | int | Construction year |
| priceMin/priceMax | int | Price range |
| zipcodeFrom/zipcodeTo | int | ZIP code range |
| municipality | int | Municipality code |
| saleType | int | Type of sale |

#### Response Structure
```json
{
  "meta": {
    "pageIndex": int,
    "pageSize": int,
    "totalCount": int,
    "totalPages": int
  },
  "results": [
    {
      "estateId": int,
      "address": string,
      "zipCode": int,
      "price": int,
      "soldDate": string,
      "propertyType": int,
      "saleType": string,
      "sqmPrice": float,
      "rooms": float,
      "size": int,
      "buildYear": int,
      "change": float,
      "latitude": float,
      "longitude": float,
      "city": string
    }
  ]
}
```

#### Headers
Required headers for requests:
```
Accept: application/json, text/plain, */*
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36
```
