db.getCollection("inventorystores").aggregate([
    {
        $match: {
            "_id": ObjectId("636cb9220c42a700123f6315")
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
            "narcoticdrugtypeuid.valuecode": { $eq: "NAR04" },
        }
    },
    {
        $lookup: {
            from: 'patientorders',
            let: {
                storeid: "$_id",
                orderid: "$itemmaster.orderitemuid"
            },
            pipeline: [
                {
                    $match: {
                        orderdate: {
                            $gte: ISODate("2024-07-01T00:00:00.000+07:00"),
                            $lte: ISODate("2024-07-01T23:59:59.999+07:00")
                        },
                        $expr: {
                            $eq: ['$$storeid', '$invstoreuid']
                        },
                    }
                },
                { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        $expr: {
                            $eq: ['$$orderid', '$patientorderitems.orderitemuid']
                        },
                    }
                },
                { $lookup: { from: "referencevalues", localField: "patientorderitems.statusuid", foreignField: "_id", as: "status" } },
                { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        "status.valuecode": { $nin: ["ORDSTS4"] }
                    }
                },
                { $unwind: { path: "$pharmacistlogs", preserveNullAndEmptyArrays: true } },
                { $lookup: { from: "users", localField: "pharmacistlogs.useruid", foreignField: "_id", as: "user" } },
                { $unwind: { path: "$user", preserveNullAndEmptyArrays: true } },
                { $lookup: { from: "referencevalues", localField: "pharmacistlogs.statusuid", foreignField: "_id", as: "statusPhar" } },
                { $unwind: { path: "$statusPhar", preserveNullAndEmptyArrays: true } },
                { $lookup: { from: "referencevalues", localField: "patientorderitems.quantityUOM", foreignField: "_id", as: "quantityUOM" } },
                { $unwind: { path: "$quantityUOM", preserveNullAndEmptyArrays: true } },
                { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "patientuid" } },
                { $unwind: { path: "$patientuid", preserveNullAndEmptyArrays: true } },
                { $lookup: { from: "referencevalues", localField: "patientuid.titleuid", foreignField: "_id", as: "title" } },
                { $unwind: { path: "$title", preserveNullAndEmptyArrays: true } },

                {
                    $addFields: {
                        detail: {
                            $cond: {
                                if: { $eq: ["$statusPhar.valuecode", "ORDSTS22"] },
                                then: "$user.name",
                                else: null
                            }
                        }
                    }
                },
                {
                    $match: {
                        "detail": { $nin: [null] }
                    }
                },
                {
                    $group: {
                        _id: "$patientorderitems._id",
                        qty: { $first: "$patientorderitems.quantity" },
                        Uom: { $first: "$quantityUOM.valuedescription" },
                        user: { $last: "$detail" },
                        hn: { $last: "$patientuid.mrn" },
                        fname: { $first: "$patientuid.firstname" },
                        lname: { $first: "$patientuid.lastname" },
                        title: { $first: "$title.valuedescription" }
                    }
                }

            ],
            as: 'orderitemdetail'
        }
    },
    { $unwind: { path: "$orderitemdetail", preserveNullAndEmptyArrays: true } },
    {
        $project: {
            orderitemdetail: 1,
            store: "$name",
            itemname: "$itemmaster.description",
            code: "$itemmaster.code"
        }
    }



])
