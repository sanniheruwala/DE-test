class Constants:
    """
    Column name for input data.
    """
    NAME = "name"
    INGREDIENTS = "ingredients"
    URL = "url"
    IMAGE = "image"
    COOKTIME = "cookTime"
    RECIPEYIELD = "recipeYield"
    DATEPUBLISHED = "datePublished"
    PREPTIME = "prepTime"
    DESCRIPTION = "description"

    """
    Derived columns.
    """
    YEAR = "year"
    COOKTIME_SEC = COOKTIME + "_sec"
    PREPTIME_SEC = PREPTIME + "_sec"
    TOTAL_COOK_TIME = "total_cook_time"
    DIFFICULTY = "difficulty"
    AVG_TOTAL_COOK_TIME = "avg_total_cooking_time"

    """
    Misc variables.
    """
    HARD = "HARD"
    EASY = "EASY"
    MEDIUM = "MEDIUM"
    OVERWRITE = "overwrite"

    """
    Filter variables.
    """
    BEEF = "beef"
