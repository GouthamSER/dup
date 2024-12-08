
from pymongo import MongoClient


# MongoDB connection details from environment
MONGODB_URI = "mongodb+srv://INLINEKUTTUBOT2:INLINEKUTTUBOT2@inlinekuttubot.vzgwwz8.mongodb.net/?retryWrites=true&w=majority&appName=INLINEKUTTUBOT"
DATABASE_NAME = "INLINEKUTTUBOT2"
COLLECTION_NAME = "files2"

# Connect to MongoDB
client = MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def delete_duplicate_files(dry_run=True):
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
    
    total_deleted = 0
    for group in duplicates:
        file_ids = group["file_ids"]
        file_ids_to_delete = file_ids[1:]  # Keep one file, delete the rest
        
        if dry_run:
            print(f"Would delete {len(file_ids_to_delete)} files of size {group['_id']}: {file_ids_to_delete}")
        else:
            result = collection.delete_many({"_id": {"$in": file_ids_to_delete}})
            print(f"Deleted {result.deleted_count} duplicate files of size {group['_id']}")
            total_deleted += result.deleted_count

    if not dry_run:
        print(f"Total duplicates removed: {total_deleted}")
    else:
        print("Dry run completed. No files were deleted.")

if __name__ == "__main__":
    # Set dry_run to False to actually delete duplicates
    delete_duplicate_files(dry_run=True)
    
