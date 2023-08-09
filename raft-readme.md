### Info on Raft's modifications to ArcadeDb for the the SOCOM DF usecase.

#### Access ArcadeDb on DF helm deployments at:
- Studio: ~/api/v1/arcadedb/studio/#
- API: ~/api/v1/arcadedb

#### Demo data
1. Give the arcadedb cluster a minute to come up. HA datasets need to be deployed onto the leader node, and a quorum needs to be established
2. To import the preloaded demo dataset in arcadedb, run the following in the sql command line in arcadedb studio

    ``````
    import database file://demo-data/FoodData_Central_foundation_food_json_2022-10-28.json with mapping = {
        "FoundationFoods": [{
            "@cat": "v",
            "@type": "<foodClass>",
            "foodNutrients": [{
                "@cat": "e",
                "@type": "HAS_NUTRIENT",
                "@in": "nutrient",
                "@cardinality": "no-duplicates",
                "nutrient": {
                    "@cat": "v",
                    "@type": "Nutrient",
                    "@id": "id",
                    "@idType": "long",
                    "@strategy": "merge"
                },
                "foodNutrientDerivation": "@ignore"
            }],
            "inputFoods": [{
                "@cat": "e",
                "@type": "INPUT",
                "@in": "inputFood",
                "@cardinality": "no-duplicates",
                "inputFood": {
                    "@cat": "v",
                    "@type": "<foodClass>",
                    "@id": "fdcId",
                    "@idType": "long",
                    "@strategy": "merge",
                    "foodCategory": {
                        "@cat": "e",
                        "@type": "HAS_CATEGORY",
                        "@cardinality": "no-duplicates",
                        "@in": {
                            "@cat": "v",
                            "@type": "FoodCategory",
                            "@id": "id",
                            "@idType": "long",
                            "@strategy": "merge"
                        }
                    }
                }
            }]
        }]
    }
    ``````
3. Run the following query to see a graph representation of the uploaded data
   ``` select *, bothE() as `@edges` from `FinalFood` ```




