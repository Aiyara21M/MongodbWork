db.getCollection("drugmasters").aggregate([
    { $lookup: { from: "referencevalues", foreignField: "_id", localField: "narcoticdrugtypeuid", as: "narcoticdrugtypeuid" } },
    { $unwind: { path: "$narcoticdrugtypeuid", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            code: {
                $in: [
                    "PICLOTG001ML",
                    "PIACUPO002ML",
                    "PIADDAO010ML",
                    "PIARTEO060MG",
                    "PIAMING0250MG",
                    "PVAMINO100ML",
                    "PIEURYG003ML",
                    "PICORDO150MG",
                    "PIAMPHG050MG",
                    "PIAVEXG020ML",
                    "PIAZADG100MG",
                    "PIBACIG00000",
                    "PIBRIDO002ML",
                    "PICALCG010PC",
                    "PIHOLDG010MG",
                    "PICARDO002MG",
                    "PICARDO010MG",
                    "PICARDOO20ML",
                    "PICERNO00000",
                    "PICISAG005ML",
                    "PICONTO040MG",
                    "PICRAVO000ML",
                    "PILEVOG500MG",
                    "PILEVOG750MG",
                    "PICEREG010ML",
                    "PICIPRG100MG",
                    "PIDESRG100MG",
                    "PIDEPAG400MG",
                    "PIDIPOO020ML",
                    "PIDOBUG250MG",
                    "PIDURAO100UG",
                    "PIESBLG100MG",
                    "PIEXPOG200UG",
                    "PIFENIG250MG",
                    "PIFLUCG100MG",
                    "PIFLUIO300MG",
                    "PIANEXO005MG",
                    "PIFUROG750MG",
                    "PSDEHEG020ML",
                    "PIGANCG500MG",
                    "PIGLYCO020ML",
                    "PIGLYCO500ML",
                    "PVGLYCO500ML",
                    "PIHERZG440MG",
                    "PIAPREO020MG",
                    "PIEPTIO010ML",
                    "PVINTRO100ML",
                    "PIISOPG001ML",
                    "PILANOO500MG",
                    "PVKIDMO200ML",
                    "PILASIO500MG",
                    "PIMERHG005GM",
                    "PIMETHG005ML",
                    "PISOMIG001GM",
                    "PIMIACO050IU",
                    "PIMYCAO050MG",
                    "PIMOBIG200UG",
                    "PIMOFAG250ML",
                    "PINALAO500UG",
                    "PINALOG004MG",
                    "PINELIG150MG",
                    "PINEXIO040MG",
                    "PINIMBO005ML",
                    "PINITRG025MG",
                    "PINOREG004MG",
                    "PINOREG004ML",
                    "PIOCTRG001MG",
                    "PIPANTG040MG",
                    "PIPAPAG002ML",
                    "PISOMIG001GM",
                    "PIPRIMO010MG",
                    "PIREMIO100MG",
                    "PISODIO050MG",
                    "PISODIG250MG",
                    "PISOLUG000ML",
                    "PISIBAG002GM",
                    "PISIBAG500MG",
                    "PITALCG004GM",
                    "PITAZRO000GM",
                    "PITAZRO045MG",
                    "PITRANO050MG",
                    "PITYGAO050MG",
                    "PITROVG250MG",
                    "PIVALOG400MG",
                    "PIVANCG500MG",
                    "PIVIB1G100MG",
                    "PIVITAG500MG",
                    "PIZAVIO020ML",
                    "PIZINFO600MG",
                    "PIZITHO500MG",
                    "PVHUMAG050ML",
                    "PVFLEXO050ML",
                    "PIXYL2O02ML"
                ]
            }
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
            "maxstocklevel": { $ifNull: ["$itemmasters.reorderdetails.maxstocklevel", 0] },
            "reorderlevel": { $ifNull: ["$itemmasters.reorderdetails.reorderlevel", 0] },
            "reorderquantity": { $ifNull: ["$itemmasters.reorderdetails.reorderquantity", 0] }
        }
    },
    {
        $addFields: {
            todate: new Date()
        }
    },
    {
        $addFields: {
            date30: { $subtract: ["$todate", 30 * 24 * 60 * 60 * 1000] }
        }
    },
    {
        $lookup: {
            from: 'stockledgers',
            let: {
                todate2: "$todate",
                itemmaster: "$itemmasters",
                date302: "$date30"
            },
            pipeline: [
                { $unwind: { path: '$ledgerdetails', preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$$itemmaster', '$itemmasteruid'] },
                                { $gte: ['$ledgerdetails.transactiondate', '$$date302'] },
                                { $lte: ['$ledgerdetails.transactiondate', '$$todate2'] }
                            ]
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
         {
                    $addFields: {
                        QTY : {$sum:"$data.quantity"},
                        QTY2: {$sum:"$data2.quantity"},
                    }
        },
{
    $unset: ["data", "data2"]  
  }

])
