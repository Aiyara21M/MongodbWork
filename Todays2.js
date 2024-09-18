db.getCollection("stockledgers").aggregate([
   { $unwind: { path: '$ledgerdetails', preserveNullAndEmptyArrays: true } },
				{
     $match: {
          "ledgerdetails.transactiondate" :{
               $gte: ISODate("2024-07-11T00:00:00.000+07:00"),
                $lte: ISODate("2024-07-11T23:59:59.999+07:00")
        },
    }
},
   { $lookup: { from: 'itemmasters', localField: 'itemmasteruid', foreignField: '_id', as: 'itemmasteruid' } },
    { $unwind: { path: '$itemmasteruid', preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "itemmasteruid.itemtypeuid": ObjectId("6352a40fbec54c73cc1d2729"),
        }
    },
    { $lookup: { from: 'inventorystores', localField: 'storeuid', foreignField: '_id', as: 'storeuid' } },
    { $unwind: { path: '$storeuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'drugmasters', localField: 'itemmasteruid.orderitemuid', foreignField: 'orderitemuid', as: 'drugmasters' } },
    { $unwind: { path: '$drugmasters', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", foreignField: "_id", localField: "drugmasters.narcoticdrugtypeuid", as: "narcoticdrugtypeuid" } },
    { $unwind: { path: "$narcoticdrugtypeuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "ledgerdetails.quantityuom", foreignField: "_id", as: "quantityUOM" } },
    { $unwind: { path: "$quantityUOM", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "ledgerdetails.quantityuom", foreignField: "_id", as: "quantityUOM" } },
    { $unwind: { path: "$quantityUOM", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "itemmasteruid.activeto": { $eq: null },
            "drugmasters.isnarcoticdrug": true,
            "ledgerdetails.transactiontype": {
                "$nin": [
                    "INVENTORIES.TRANSFERIN",
                    "INVENTORIES.GOODSRECEIVE",
                    "INVENTORIES.TRANSFEROUT"
                ]
            },
        }
    },
    {
        $project: {
            "_id": "$drugmasters.name",
            "drugdetails": "$drugmasters.name",
            "quantity": "$ledgerdetails.quantity",
            "narcoticdrugtypeuid": "$narcoticdrugtypeuid.valuedescription",
            "quantityUOM": "$quantityUOM.valuedescription",
        }
    },
//    {
//        $group: {
//            "_id": {
//                "drugdetails": "$drugdetails",
//                "narcoticdrugtypeuid": "$narcoticdrugtypeuid",
//                "quantityUOM": "$quantityUOM",
//                 "vfromdate":  ISODate("2024-07-12T00:00:00.000+07:00"),
//				 "vtodate": ISODate("2024-07-12T23:59:59.999+07:00")
//            },
//            "quantity": { $sum: "$quantity" }
//        }
//    },




])
