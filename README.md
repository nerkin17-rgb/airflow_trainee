## DAGs

<img width="1910" height="915" alt="dag" src="https://github.com/user-attachments/assets/3675fc07-884c-40a8-bf5a-e6760bd0ac40" />
<img width="1910" height="915" alt="dag1" src="https://github.com/user-attachments/assets/15f88002-eb18-4b6b-a43b-c79300113b9f" />

## MongoDB Queries
### Top 5 frequently occurring comments
~~~
db.processed_comments.aggregate([
  { $group: { _id: "$content", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
])
~~~
<img width="1920" height="1020" alt="1" src="https://github.com/user-attachments/assets/36ac2c0f-71a5-4b2b-8504-415e656024f8" />

### All entries where the “content” field is less than 5 characters long
~~~
db.processed_comments.find({
  $expr: { $lt: [ { $strLenCP: "$content" }, 5 ] }
})
~~~
<img width="1920" height="1020" alt="2" src="https://github.com/user-attachments/assets/66a64e6f-47fa-4dd6-b59d-b0948f6a6d90" />

### Average rating for each day
~~~
db.processed_comments.aggregate([
  { $group: { _id: { $substr: ["$at", 0, 10] }, avg_rating: { $avg: "$score" } } },
  { $sort: { _id: 1 } },
  { $project: { day: "$_id", avg_rating: 1, timestamp: { $toDate: "$_id" } } }
])
~~~
<img width="1920" height="1020" alt="3" src="https://github.com/user-attachments/assets/75cae34f-61f4-4ce8-9e8b-1347c636d0f5" />

