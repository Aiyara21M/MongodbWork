db.getCollection("drugmasters").aggregate([

    { $lookup: { from: "referencevalues", foreignField: "_id", localField: "narcoticdrugtypeuid", as: "narcoticdrugtypeuid" } },
    { $unwind: { path: "$narcoticdrugtypeuid", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            code: { $in: ["PIACUPO002ML", "PIAMKIG012GM", "PIAMPHG050MG", "PIAVEXG020ML", "PIBACIG00000", "PIBRIDO002ML", "PICALCG010PC", "PICARDO002MG", "PICARDOO20ML", "PICONTO040MG"] }
        }
    },
    { $lookup: { from: 'itemmasters', localField: 'orderitemuid', foreignField: 'orderitemuid', as: 'itemmasters' } },
    { $unwind: { path: '$itemmasters', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$itemmasters.reorderdetails', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "inventorystores", localField: "itemmasters.reorderdetails.storeuid", foreignField: "_id", as: "store" } },
    { $unwind: { path: "$store", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "store.code": "PHAIPD"
        }
    },
    {
        $project: {
            "_id": "$_id",
            "drugdetails": "$name",
            "narcoticdrugtypeuid": "$narcoticdrugtypeuid.valuedescription",
            "itemmasters": "$itemmasters._id",
            "maxstocklevel": "$itemmasters.reorderdetails.maxstocklevel",
            "reorderlevel": "$itemmasters.reorderdetails.reorderlevel",
            "reorderquantity": "$itemmasters.reorderdetails.reorderquantity"
        }
    },
    {
        $lookup: {
            from: 'stockledgers',
            let: {
                itemmaster: "$itemmasters",
            },
            pipeline: [
                { $unwind: { path: '$ledgerdetails', preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        $expr: {
                            $eq: ['$$itemmaster', '$itemmasteruid'],
                        },
                        "ledgerdetails.transactiondate": {
                            "$gte": ISODate("2024-06-19T00:00:00.000+07:00"),
                            "$lte": ISODate("2024-07-19T23:59:59.999+07:00")
                        }
                    }
                },
                { $lookup: { from: "referencevalues", localField: "ledgerdetails.quantityuom", foreignField: "_id", as: "quantityUOM" } },
                { $unwind: { path: "$quantityUOM", preserveNullAndEmptyArrays: true } },
                { $lookup: { from: "referencevalues", localField: "ledgerdetails.quantityuom", foreignField: "_id", as: "quantityUOM" } },
                { $unwind: { path: "$quantityUOM", preserveNullAndEmptyArrays: true } },
                {
                    $project: {
                        "_id": "$itemmasteruid",
                        "quantity": "$ledgerdetails.quantity",
                        "quantityUOM": "$quantityUOM.valuedescription",
                    }
                },
                {
                    $group: {
                        "_id": {
                            "_id": "$_id",
                            "quantityUOM": "$quantityUOM",
                        },
                        "quantity": { $sum: "$quantity" }
                    }
                },
            ],
            as: 'data'
        }
    },
    {
        $lookup: {
            from: 'stockledgers',
            let: {
                itemmaster: "$itemmasters",
            },
            pipeline: [
                { $unwind: { path: '$ledgerdetails', preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        $expr: {
                            $eq: ['$$itemmaster', '$itemmasteruid'],
                        },
                    }
                },
                { $lookup: { from: "referencevalues", localField: "ledgerdetails.quantityuom", foreignField: "_id", as: "quantityUOM" } },
                { $unwind: { path: "$quantityUOM", preserveNullAndEmptyArrays: true } },
                {
                    $project: {
                        "_id": "$itemmasteruid",
                        "quantity": "$ledgerdetails.quantity",
                        "quantityUOM": "$quantityUOM.valuedescription",
                    }
                },
                {
                    $group: {
                        "_id": {
                            "_id": "$_id",
                            "quantityUOM": "$quantityUOM",
                        },
                        "quantity": { $sum: "$quantity" }
                    }
                },
            ],
            as: 'data2'
        }
    },
    { $unwind: { path: "$data", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$data2", preserveNullAndEmptyArrays: true } },



])
