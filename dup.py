from pymongo import MongoClient

# MongoDB connection details
client = MongoClient("mongodb+srv://INLINEKUTTUBOT2:INLINEKUTTUBOT2@inlinekuttubot.vzgwwz8.mongodb.net/?retryWrites=true&w=majority&appName=INLINEKUTTUBOT")
db = client["INLINEKUTTUBOT2"]
collection = db["files2"]

def delete_duplicate_files():
    # Group files by size and keep only one unique file per size
    pipeline = [
        {
            "$group": {
                "_id": "$size",  # Group by file size
                "file_ids": {"$push": "$_id"},  # Collect all IDs with the same size
                "count": {"$sum": 1}  # Count the number of files with the same size
            }
        },
        {
            "$match": {"count": {"$gt": 1}}  # Only consider sizes with duplicates
        }
    ]
    
    duplicates = list(collection.aggregate(pipeline))
    
    for group in duplicates:
        file_ids = group["file_ids"]
        # Keep one file ID and delete the rest
        file_ids_to_delete = file_ids[1:]  # Exclude the first file ID
        
        result = collection.delete_many({"_id": {"$in": file_ids_to_delete}})
        print(f"Deleted {result.deleted_count} duplicate files of size {group['_id']}")

if __name__ == "__main__":
    delete_duplicate_files()
    print("Duplicate files removed.")
