db.getCollection("patientpackages").aggregate([
{ $unwind: { path: "$orderitems", preserveNullAndEmptyArrays: true } },
{ $lookup: { from: "orderitems", localField: "orderitems.orderitemuid", foreignField: "_id", as: "item" } },
{ $unwind: { path: "$item", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "ordercategories", localField: "item.ordersubcatuid", foreignField: "_id", as: "subcat" } },
    { $unwind: { path: "$subcat", preserveNullAndEmptyArrays: true } },
        {
        $match: {
//            "type.relatedvalue": { $eq: "IPD" },
//            "status.valuedescription": { $ne: "Cancelled" },
            "subcat.name": { $regex: /^DF in order set/i },
        }
    },
])