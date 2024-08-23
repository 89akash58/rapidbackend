const { MongoClient } = require("mongodb");
const express = require("express");
const uri = process.env.MONGODB_URI;

const client = new MongoClient(uri);
const db = client.db("RQ_Analytics");
const app = express.Router();

// connectToDatabase();

//Total sales over time
app.get("/total_sales/:interval", async (req, res) => {
  try {
    const { interval } = req.params;
    const dateFormat = {
      daily: "%Y-%m-%d",
      monthly: "%Y-%m",
      quarterly: "%Y-Q%q",
      yearly: "%Y",
    }[interval];

    const pipeline = [
      {
        $addFields: {
          created_at: {
            $cond: {
              if: { $eq: [{ $type: "$created_at" }, "date"] },
              then: "$created_at",
              else: { $dateFromString: { dateString: "$created_at" } },
            },
          },
        },
      },
      {
        $group: {
          _id: {
            $dateToString: {
              format: dateFormat,
              date: "$created_at",
            },
          },
          total_sales: {
            $sum: { $toDouble: "$total_price_set.shop_money.amount" },
          },
        },
      },
      { $sort: { _id: 1 } },
    ];

    const result = await db
      .collection("shopifyOrders")
      .aggregate(pipeline)
      .toArray();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      error: "An error occurred while fetching data",
      details: error.message,
    });
  }
});

// 2. Sales Growth Rate Over Time
app.get("/sales_growth/:interval", async (req, res) => {
  try {
    const { interval } = req.params;
    const dateFormat = {
      daily: "%Y-%m-%d",
      monthly: "%Y-%m",
      quarterly: "%Y-Q%q",
      yearly: "%Y",
    }[interval];

    const pipeline = [
      {
        $addFields: {
          created_at: {
            $cond: {
              if: { $eq: [{ $type: "$created_at" }, "date"] },
              then: "$created_at",
              else: { $dateFromString: { dateString: "$created_at" } },
            },
          },
        },
      },
      {
        $group: {
          _id: {
            $dateToString: {
              format: dateFormat,
              date: "$created_at",
            },
          },
          total_sales: {
            $sum: { $toDouble: "$total_price_set.shop_money.amount" },
          },
        },
      },
      { $sort: { _id: 1 } },
      {
        $group: {
          _id: null,
          sales: { $push: { date: "$_id", total: "$total_sales" } },
        },
      },
      {
        $project: {
          _id: 0,
          growth_rates: {
            $map: {
              input: { $range: [1, { $size: "$sales" }] },
              as: "i",
              in: {
                date: { $arrayElemAt: ["$sales.date", "$$i"] },
                growth_rate: {
                  $multiply: [
                    {
                      $divide: [
                        {
                          $subtract: [
                            { $arrayElemAt: ["$sales.total", "$$i"] },
                            {
                              $arrayElemAt: [
                                "$sales.total",
                                { $subtract: ["$$i", 1] },
                              ],
                            },
                          ],
                        },
                        {
                          $arrayElemAt: [
                            "$sales.total",
                            { $subtract: ["$$i", 1] },
                          ],
                        },
                      ],
                    },
                    100,
                  ],
                },
              },
            },
          },
        },
      },
    ];

    const result = await db
      .collection("shopifyOrders")
      .aggregate(pipeline)
      .toArray();
    res.json(result[0]?.growth_rates || []);
  } catch (error) {
    res.status(500).json({
      error: "An error occurred while fetching sales growth data",
      details: error.message,
    });
  }
});

//3. New customer added over time
app.get("/new_customers/:interval", async (req, res) => {
  try {
    const { interval } = req.params;
    const dateFormat = {
      daily: "%Y-%m-%d",
      monthly: "%Y-%m",
      quarterly: "%Y-Q%q",
      yearly: "%Y",
    }[interval];

    const pipeline = [
      {
        $addFields: {
          created_at: {
            $cond: {
              if: { $eq: [{ $type: "$created_at" }, "date"] },
              then: "$created_at",
              else: { $dateFromString: { dateString: "$created_at" } },
            },
          },
        },
      },
      {
        $group: {
          _id: {
            $dateToString: {
              format: dateFormat,
              date: "$created_at",
            },
          },
          count: { $sum: 1 },
        },
      },
      { $sort: { _id: 1 } },
    ];

    const result = await db
      .collection("shopifyCustomers")
      .aggregate(pipeline)
      .toArray();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      error: "An error occurred while fetching data",
      details: error.message,
    });
  }
});

// 4. Number of Repeat Customers
app.get("/repeat_customers/:interval", async (req, res) => {
  try {
    const { interval } = req.params;
    const dateFormat = {
      daily: "%Y-%m-%d",
      monthly: "%Y-%m",
      quarterly: "%Y-Q%q",
      yearly: "%Y",
    }[interval];

    const pipeline = [
      {
        $addFields: {
          created_at: {
            $cond: {
              if: { $eq: [{ $type: "$created_at" }, "date"] },
              then: "$created_at",
              else: { $dateFromString: { dateString: "$created_at" } },
            },
          },
        },
      },
      {
        $group: {
          _id: {
            customer: "$customer.id",
            date: {
              $dateToString: {
                format: dateFormat,
                date: "$created_at",
              },
            },
          },
          count: { $sum: 1 },
        },
      },
      {
        $group: {
          _id: "$_id.date",
          repeat_customers: {
            $sum: { $cond: [{ $gt: ["$count", 1] }, 1, 0] },
          },
        },
      },
      { $sort: { _id: 1 } },
    ];

    const result = await db
      .collection("shopifyOrders")
      .aggregate(pipeline)
      .toArray();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      error: "An error occurred while fetching repeat customers data",
      details: error.message,
    });
  }
});

// 5. Geographical Distribution of Customers
app.get("/customer_geography", async (req, res) => {
  try {
    const pipeline = [
      {
        $group: {
          _id: "$default_address.city",
          count: { $sum: 1 },
        },
      },
      { $sort: { count: -1 } },
      { $limit: 20 }, // Limiting to top 20 cities for example
    ];

    const result = await db
      .collection("shopifyCustomers")
      .aggregate(pipeline)
      .toArray();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      error: "An error occurred while fetching customer geography data",
    });
  }
});

// 6. Customer Lifetime Value by Cohorts
app.get("/customer_ltv", async (req, res) => {
  try {
    const pipeline = [
      {
        $lookup: {
          from: "shopifyCustomers",
          let: { customerId: "$customer.id" },
          pipeline: [{ $match: { $expr: { $eq: ["$id", "$$customerId"] } } }],
          as: "customer_info",
        },
      },
      { $unwind: { path: "$customer_info", preserveNullAndEmptyArrays: true } },
      {
        $group: {
          _id: {
            customer_id: "$customer.id",
            cohort: {
              $dateToString: {
                format: "%Y-%m",
                date: {
                  $cond: {
                    if: { $ifNull: ["$customer_info.created_at", false] },
                    then: {
                      $dateFromString: {
                        dateString: "$customer_info.created_at",
                        onError: "$created_at",
                      },
                    },
                    else: {
                      $dateFromString: {
                        dateString: "$created_at",
                        onError: new Date(0),
                      },
                    },
                  },
                },
              },
            },
          },
          total_spent: {
            $sum: {
              $cond: [
                { $ifNull: ["$total_price_set.shop_money.amount", false] },
                { $toDouble: "$total_price_set.shop_money.amount" },
                0,
              ],
            },
          },
        },
      },
      {
        $group: {
          _id: "$_id.cohort",
          avg_ltv: { $avg: "$total_spent" },
          customer_count: { $sum: 1 },
        },
      },
      { $sort: { _id: 1 } },
    ];

    const result = await db
      .collection("shopifyOrders")
      .aggregate(pipeline)
      .toArray();
    res.json(result);
  } catch (error) {
    console.error("Error in customer_ltv endpoint:", error);
    res.status(500).json({
      error: "An error occurred while fetching customer LTV data",
      details: error.message,
    });
  }
});

// Add more routes for other charts...
module.exports = app;
