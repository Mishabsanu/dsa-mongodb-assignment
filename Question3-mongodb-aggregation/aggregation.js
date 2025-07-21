db.sales.aggregate([
  {
    $unwind: "$items",
  },
  {
    $addFields: {
      month: { $dateToString: { format: "%Y-%m", date: "$date" } },
      itemRevenue: { $multiply: ["$items.quantity", "$items.price"] },
    },
  },
  {
    $group: {
      _id: { store: "$store", month: "$month" },
      totalRevenue: { $sum: "$itemRevenue" },
      totalPrice: { $sum: "$items.price" },
      itemCount: { $sum: 1 },
    },
  },
  {
    $project: {
      _id: 0,
      store: "$_id.store",
      month: "$_id.month",
      totalRevenue: { $round: ["$totalRevenue", 1] },
      averagePrice: {
        $cond: [
          { $eq: ["$itemCount", 0] },
          0.0,
          { $round: [{ $divide: ["$totalPrice", "$itemCount"] }, 1] },
        ],
      },
    },
  },

  {
    $sort: {
      store: 1,
      month: 1,
    },
  },
]);
