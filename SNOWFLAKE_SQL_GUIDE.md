
# Step-by-Step Guide to Setting Up a Nutrition Database with Cortex Search Service

### Step 1: Create a Database and Schema
```sql
CREATE DATABASE nutrition;
CREATE SCHEMA data;
```

---

### Step 2: Create a Stage for Document Upload
```sql
CREATE OR REPLACE STAGE nutrition_data_stage 
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') 
DIRECTORY = (ENABLE = true);
```

---

### Step 3: Create a Table and Upload Your Data
1. Select **Data** on the left of Snowsight.
2. Click **ADD DATA** and then select **Load Data into a Table**.
3. Browse your files and configure the table name (e.g., `TABLE2`).
   - **Note:** Supported formats are CSV/TSV, JSON, ORC, Avro, or Parquet.

---

### Step 4: Verify Data Loaded into the Table
```sql
SELECT * FROM TABLE2;
```

---

### Step 5: Create a Table for Food Categories
```sql
CREATE OR REPLACE TABLE FOOD_CATEGORIES (
    FOOD_ITEM STRING,
    CATEGORY STRING
);
```

---

### Step 6: Categorize Food Items Using `COMPLETE` Function
```sql
INSERT INTO FOOD_CATEGORIES (FOOD_ITEM, CATEGORY)
SELECT 
    C2 AS FOOD_ITEM, 
    TRIM(snowflake.cortex.COMPLETE(
        'mistral-large',
        'The food item is: ' || C2 || '. Its nutritional profile is as follows: Calories=' || C4 || 
        ', Protein=' || C39 || ', Carbohydrates=' || C59 || ', Fats=' || C68 || ', Fiber=' || C60 || 
        ', Sodium=' || C8 || ', Iron=' || C32 || ', Magnesium=' || C33 || ', Calcium=' || C30 || 
        ', Vitamin A=' || C16 || ', Vitamin B6=' || C23 || ', Vitamin B12=' || C24 || ', Vitamin C=' || C25 || ', Vitamin D=' || C26 || 
        ', Vitamin E=' || C27 || ', Vitamin K=' || C29 ||  
        '. Categorize the food item as high in protein, carbohydrates, fats, fiber, or essential minerals. Return the category only; do not include the name of the food item.'
    )) AS CATEGORY
FROM TABLE1;
```

---

### Step 7: Add Categories to an Existing Table
1. Add a `Category` column to `TABLE2`:
    ```sql
    ALTER TABLE TABLE2 ADD COLUMN Category STRING;
    ```
2. Update `TABLE2` with categories from `FOOD_CATEGORIES`:
    ```sql
    UPDATE TABLE2
    SET category = FOOD_CATEGORIES.CATEGORY
    FROM FOOD_CATEGORIES
    WHERE TABLE2.NAME = FOOD_CATEGORIES.FOOD_ITEM;
    ```
3. Verify the distinct food items in `FOOD_CATEGORIES`:
    ```sql
    SELECT DISTINCT FOOD_ITEM FROM FOOD_CATEGORIES;
    ```

---

### Step 8: Create a Cortex Search Service for Your Data
```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE NUTRITION_SEARCH
ON (
    CATEGORY
)
ATTRIBUTES CALORIES, TOTAL_FAT, CHOLESTEROL, SODIUM, VITAMIN_A, VITAMIN_B12, VITAMIN_B6, VITAMIN_C, VITAMIN_D, VITAMIN_E, VITAMIN_K, CALCIUM, IRON, POTASSIUM, SELENIUM, PROTEIN, CARBOHYDRATE
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 minute'
AS (
    SELECT
        NAME, SERVING_SIZE, CALORIES, TOTAL_FAT, SATURATED_FAT, CHOLESTEROL, SODIUM, CHOLINE, FOLATE, FOLIC_ACID, NIACIN, PANTOTHENIC_ACID, RIBOFLAVIN, THIAMIN, VITAMIN_A, VITAMIN_A_RAE,
        CAROTENE_ALPHA, CAROTENE_BETA, CRYPTOXANTHIN_BETA, LUTEIN_ZEAXANTHIN, LYCOPENE, VITAMIN_B12, VITAMIN_B6, VITAMIN_C, VITAMIN_D, VITAMIN_E, TOCOPHEROL_ALPHA, VITAMIN_K, CALCIUM,
        COPPER, IRON, MAGNESIUM, MANGANESE, PHOSPHOROUS, POTASSIUM, SELENIUM, ZINC, PROTEIN, ALANINE, ARGININE, ASPARTIC_ACID, CYSTINE, GLUTAMIC_ACID, GLYCINE, HISTIDINE, HYDROXYPROLINE,
        ISOLEUCINE, LEUCINE, LYSINE, METHIONINE, PHENYLALANINE, PROLINE, SERINE, THREONINE, TRYPTOPHAN, TYROSINE, VALINE, CARBOHYDRATE, FIBER, SUGARS, FRUCTOSE, GALACTOSE, GLUCOSE, LACTOSE,
        MALTOSE, SUCROSE, FAT, SATURATED_FATTY_ACIDS, MONOUNSATURATED_FATTY_ACIDS, POLYUNSATURATED_FATTY_ACIDS, FATTY_ACIDS_TOTAL_TRANS, ALCOHOL, ASH, CAFFEINE, THEOBROMINE, WATER,
        CATEGORY
    FROM "NUTRITION"."DATA"."TABLE2"
);
```

---
