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
    const validIntervals = ["daily", "monthly", "quarterly", "yearly"];

    if (!validIntervals.includes(interval)) {
      return res.status(400).json({ error: "Invalid interval parameter" });
    }

    const data = await db.collection("shopifyOrders").find({}).toArray();
    const processedData = processSalesData(data, interval);

    res.json(processedData);
  } catch (error) {
    res.status(500).json({
      error: "An error occurred while fetching data",
      details: error.message,
    });
  }
});

function processSalesData(data, interval) {
  const result = {};

  data.forEach((doc) => {
    const date = new Date(doc.created_at);
    const year = date.getFullYear();
    const amount = parseFloat(doc.total_price_set.shop_money.amount);

    let period;
    switch (interval) {
      case "daily":
        period = `${year}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(date.getDate()).padStart(2, "0")}`;
        break;
      case "monthly":
        period = `${year}-${String(date.getMonth() + 1).padStart(2, "0")}`;
        break;
      case "quarterly":
        const quarter = Math.ceil((date.getMonth() + 1) / 3);
        period = `${year}-Q${quarter}`;
        break;
      case "yearly":
        period = `${year}`;
        break;
      default:
        period = `${year}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(date.getDate()).padStart(2, "0")}`;
    }

    if (!result[period]) {
      result[period] = 0;
    }
    result[period] += amount; // Sum up the total sales
  });

  return Object.entries(result).map(([period, total_sales]) => ({
    period,
    total_sales,
  }));
}

// 2. Sales Growth Rate Over Time
app.get("/sales_growth/:interval", async (req, res) => {
  try {
    const { interval } = req.params;
    const validIntervals = ["daily", "monthly", "quarterly", "yearly"];

    if (!validIntervals.includes(interval)) {
      return res.status(400).json({ error: "Invalid interval parameter" });
    }

    const data = await db.collection("shopifyOrders").find({}).toArray();
    const processedData = processSalesGrowthData(data, interval);

    res.json(processedData);
  } catch (error) {
    res.status(500).json({
      error: "An error occurred while fetching sales growth data",
      details: error.message,
    });
  }
});

function processSalesGrowthData(data, interval) {
  const result = {};

  data.forEach((doc) => {
    const date = new Date(doc.created_at);
    const year = date.getFullYear();
    const amount = parseFloat(doc.total_price_set.shop_money.amount);

    let period;
    switch (interval) {
      case "daily":
        period = `${year}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(date.getDate()).padStart(2, "0")}`;
        break;
      case "monthly":
        period = `${year}-${String(date.getMonth() + 1).padStart(2, "0")}`;
        break;
      case "quarterly":
        const quarter = Math.ceil((date.getMonth() + 1) / 3);
        period = `${year}-Q${quarter}`;
        break;
      case "yearly":
        period = `${year}`;
        break;
      default:
        period = `${year}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(date.getDate()).padStart(2, "0")}`;
    }

    if (!result[period]) {
      result[period] = 0;
    }
    result[period] += amount; // Sum up the total sales
  });

  const sortedPeriods = Object.keys(result).sort();
  const salesArray = sortedPeriods.map((period) => ({
    date: period,
    total: result[period],
  }));

  // Calculate growth rates
  const growthRates = salesArray.slice(1).map((current, index) => {
    const previous = salesArray[index];
    const growth_rate =
      previous.total === 0
        ? 0
        : ((current.total - previous.total) / previous.total) * 100;
    return {
      date: current.date,
      growth_rate: growth_rate.toFixed(2),
    };
  });

  return growthRates;
}

//3. New customer added over time
function processIntervalData(data, interval) {
  const result = {};

  data.forEach((doc) => {
    const date = new Date(doc.created_at);
    const year = date.getFullYear();
    let period;

    switch (interval) {
      case "daily":
        period = `${year}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(date.getDate()).padStart(2, "0")}`;
        break;
      case "monthly":
        period = `${year}-${String(date.getMonth() + 1).padStart(2, "0")}`;
        break;
      case "quarterly":
        const quarter = Math.ceil((date.getMonth() + 1) / 3);
        period = `${year}-Q${quarter}`;
        break;
      case "yearly":
        period = `${year}`;
        break;
      default:
        period = `${year}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(date.getDate()).padStart(2, "0")}`;
    }

    if (!result[period]) {
      result[period] = 0;
    }
    result[period] += 1; // Count customers
  });

  return Object.entries(result).map(([period, count]) => ({ period, count }));
}

app.get("/new_customers/:interval", async (req, res) => {
  try {
    const { interval } = req.params;
    const validIntervals = ["daily", "monthly", "quarterly", "yearly"];

    if (!validIntervals.includes(interval)) {
      return res.status(400).json({ error: "Invalid interval parameter" });
    }

    const data = await db.collection("shopifyCustomers").find({}).toArray();

    // Process the data based on the interval
    const processedData = processIntervalData(data, interval);

    res.json(processedData);
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
    const validIntervals = ["daily", "monthly", "quarterly", "yearly"];

    if (!validIntervals.includes(interval)) {
      return res.status(400).json({ error: "Invalid interval parameter" });
    }

    const data = await db.collection("shopifyOrders").find({}).toArray();
    const processedData = processRepeatCustomersData(data, interval);

    res.json(processedData);
  } catch (error) {
    res.status(500).json({
      error: "An error occurred while fetching repeat customers data",
      details: error.message,
    });
  }
});

function processRepeatCustomersData(data, interval) {
  const customerPeriods = {};

  data.forEach((doc) => {
    const date = new Date(doc.created_at);
    const year = date.getFullYear();
    const customerId = doc.customer.id;
    const amount = parseFloat(doc.total_price_set.shop_money.amount);

    let period;
    switch (interval) {
      case "daily":
        period = `${year}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(date.getDate()).padStart(2, "0")}`;
        break;
      case "monthly":
        period = `${year}-${String(date.getMonth() + 1).padStart(2, "0")}`;
        break;
      case "quarterly":
        const quarter = Math.ceil((date.getMonth() + 1) / 3);
        period = `${year}-Q${quarter}`;
        break;
      case "yearly":
        period = `${year}`;
        break;
      default:
        period = `${year}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(date.getDate()).padStart(2, "0")}`;
    }

    if (!customerPeriods[customerId]) {
      customerPeriods[customerId] = {};
    }
    if (!customerPeriods[customerId][period]) {
      customerPeriods[customerId][period] = 0;
    }
    customerPeriods[customerId][period] += 1;
  });

  const result = {};
  for (const customerId in customerPeriods) {
    for (const period in customerPeriods[customerId]) {
      if (!result[period]) {
        result[period] = 0;
      }
      result[period] += 1;
    }
  }

  return Object.entries(result).map(([period, count]) => ({
    period,
    repeat_customers: count,
  }));
}

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
