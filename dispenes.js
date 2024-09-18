db.getCollection("inventorystores").aggregate([
    {
        $match: {
  "_id" : ObjectId("636cb9220c42a700123f6315")
        }
    },
    {
        $lookup: {
            from: 'itemmasters',
            let: {
                storeid: "$_id",
            },
            pipeline: [
                { $unwind: { path: "$handlingstores", preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        $expr: {
                            $eq: ['$$storeid', '$handlingstores.storeuid']
                        },
                    }
                },
            ],
            as: 'itemmaster'
        }
    },
    { $unwind: { path: "$itemmaster", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'drugmasters', localField: 'itemmaster.orderitemuid', foreignField: 'orderitemuid', as: 'drugmasters' } },
    { $unwind: { path: '$drugmasters', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "drugmasters.narcoticdrugtypeuid", foreignField: "_id", as: "narcoticdrugtypeuid" } },
    { $unwind: { path: "$narcoticdrugtypeuid", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "narcoticdrugtypeuid.valuecode": { $nin: [null] },
            "itemmaster.activeto": { $eq: null },
            "narcoticdrugtypeuid.valuecode": { $eq: "NAR01" },
        }
    },
    {
        $lookup: {
            from: 'stockdispenses',
            let: {
                storeid: "$_id",
                orderid: "$itemmaster.orderitemuid"
            },
            pipeline: [
                {
                    $match: {
                        dispensedate: {
                            $gte: ISODate("2024-07-01T00:00:00.000+07:00"),
                            $lte: ISODate("2024-07-01T23:59:59.999+07:00")
                        },
                        $expr: {
                            $eq: ['$$storeid', '$fromstoreuid'],
                        },
                    }
                },
                { $unwind: { path: "$itemdetails", preserveNullAndEmptyArrays: true } },
                { $lookup: { from: 'itemmasters', localField: 'itemdetails.itemmasteruid', foreignField: '_id', as: 'itemmaster' } },
                { $unwind: { path: '$itemmaster', preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        $expr: {
                            $eq: ['$$orderid', '$itemmaster.orderitemuid'],
                        },
                    }
                },
            ],
            as: 'dispensesdetail'
        }
    },
    { $unwind: { path: '$dispensesdetail', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "dispensesdetail.itemdetails.quantityuom", foreignField: "_id", as: "quantityUOM" } },
    { $unwind: { path: "$quantityUOM", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "patients", localField: "dispensesdetail.patientuid", foreignField: "_id", as: "patient" } },
    { $unwind: { path: "$patient", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "referencevalues", localField: "patient.titleuid", foreignField: "_id", as: "title" } },
    { $unwind: { path: "$title", preserveNullAndEmptyArrays: true } },
    
//{
//    $project:{
//        store:"$name",
//        uom:"$quantityUOM.valuedescription",
//        qty:"$dispensesdetail.itemdetails.quantity" ,
//        title:"$title.valuedescription",
        fname:"$patient.firstname",{$}
//        lname:{$ifNull}
//    }
//}


])
