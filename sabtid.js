db.getCollection("stockledgers").aggregate([
   { $unwind: { path: '$ledgerdetails', preserveNullAndEmptyArrays: true } },
				{
     $match: {
          "ledgerdetails.transactiondate" :{
               $gte: ISODate("2024-07-10T00:00:00.000+07:00"),
                $lte: ISODate("2024-07-10T23:59:59.999+07:00")
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
        { $lookup: { from: "patients", localField: "ledgerdetails.patientuid", foreignField: "_id", as: "patient" } },
    { $unwind: { path: "$patient", preserveNullAndEmptyArrays: true } },
            { $lookup: { from: "users", localField: "ledgerdetails.transactionuseruid", foreignField: "_id", as: "user" } },
    { $unwind: { path: "$user", preserveNullAndEmptyArrays: true } },
    
    {
        $match: {
            "narcoticdrugtypeuid.valuecode":"NAR04",
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
        $project:{
            "store":"$storeuid.name",
            "itemname":"$itemmasteruid.description",
            "itemcode":"$itemmasteruid.code",
            "quantity":"$ledgerdetails.quantity",
           "uom":"$quantityUOM.valuedescription",
             "user":"$user.name",
                "hn":"$patient.mrn",
                   "fname":"$patient.firstname",
                    "lname":{$ifNull: [ "$patient.lastname", ""]}
        }
    }
    
])
