// queries.js - MongoDB CRUD, Advanced Queries, Aggregations, and Indexing

const { MongoClient } = require('mongodb');
const uri = 'mongodb://localhost:27017'; // or your MongoDB Atlas connection string
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    /*
     * ===============================
     * 🧱 Task 2: Basic CRUD Operations
     * ===============================
     */

    // 1️⃣ Find all books in a specific genre
    console.log("\n1️⃣ Books in the Fiction genre:");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    // 2️⃣ Find books published after a certain year
    console.log("\n2️⃣ Books published after 2000:");
    console.log(await books.find({ published_year: { $gt: 2000 } }).toArray());

    // 3️⃣ Find books by a specific author
    console.log("\n3️⃣ Books by George Orwell:");
    console.log(await books.find({ author: "George Orwell" }).toArray());

    // 4️⃣ Update the price of a specific book
    console.log("\n4️⃣ Updating price of '1984'...");
    await books.updateOne(
      { title: "1984" },
      { $set: { price: 15.99 } }
    );
    console.log(await books.findOne({ title: "1984" }));

    // 5️⃣ Delete a book by its title
    console.log("\n5️⃣ Deleting 'Moby Dick'...");
    await books.deleteOne({ title: "Moby Dick" });
    console.log("Deleted 'Moby Dick' successfully");

    /*
     * ==================================
     * 🔍 Task 3: Advanced Query Features
     * ==================================
     */

    // 6️⃣ Find books that are both in stock and published after 2010
    console.log("\n6️⃣ In-stock books published after 2010:");
    console.log(await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray());

    // 7️⃣ Use projection (only return title, author, and price)
    console.log("\n7️⃣ Books showing only title, author, and price:");
    console.log(await books.find({}, { projection: { _id: 0, title: 1, author: 1, price: 1 } }).toArray());

    // 8️⃣ Sorting by price ascending
    console.log("\n8️⃣ Books sorted by price (ascending):");
    console.log(await books.find().sort({ price: 1 }).limit(5).toArray());

    // 9️⃣ Sorting by price descending
    console.log("\n9️⃣ Books sorted by price (descending):");
    console.log(await books.find().sort({ price: -1 }).limit(5).toArray());

    // 🔟 Pagination (5 books per page)
    const page = 2; // Change this to 1, 2, 3... to test
    const pageSize = 5;
    const skip = (page - 1) * pageSize;
    console.log(`\n🔟 Page ${page} (5 books per page):`);
    console.log(await books.find().skip(skip).limit(pageSize).toArray());

    /*
     * =============================
     * 📊 Task 4: Aggregation Pipelines
     * =============================
     */

    // 1️⃣ Average price of books by genre
    console.log("\n📊 Average price of books by genre:");
    console.log(await books.aggregate([
      { $group: { _id: "$genre", averagePrice: { $avg: "$price" } } }
    ]).toArray());

    // 2️⃣ Author with the most books
    console.log("\n📊 Author with the most books:");
    console.log(await books.aggregate([
      { $group: { _id: "$author", bookCount: { $sum: 1 } } },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray());

    // 3️⃣ Group books by publication decade and count them
    console.log("\n📊 Books grouped by publication decade:");
    console.log(await books.aggregate([
      {
        $project: {
          decade: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] }
        }
      },
      {
        $group: {
          _id: "$decade",
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray());

    /*
     * =========================
     * ⚙️ Task 5: Indexing
     * =========================
     */

    // 1️⃣ Create an index on title
    console.log("\n⚙️ Creating index on title...");
    await books.createIndex({ title: 1 });

    // 2️⃣ Create compound index on author and published_year
    console.log("⚙️ Creating compound index on author + published_year...");
    await books.createIndex({ author: 1, published_year: 1 });

    // 3️⃣ Use explain() to demonstrate performance
    console.log("\n📈 Explain output for indexed query (find by title):");
    const explainResult = await books.find({ title: "1984" }).explain("executionStats");
    console.log(JSON.stringify(explainResult.executionStats, null, 2));

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log("\n🔚 Connection closed");
  }
}

runQueries().catch(console.error);
