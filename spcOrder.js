db.getCollection("patientvisits").aggregate([
    {
        $match: {
            _id: ObjectId("6541c6afcb555a00017277b4")
        }
    },
    {
        $lookup: {
            from: 'patientorders',
            let: {
                visitid: "$_id",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$visitid', '$patientvisituid']
                        },
                    }
                },
                { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        "patientorderitems.chargecode": { $in: ["EX001", "EX022", "EX011", "EX020", "E016", "EX012", "EX004", "EX008", "EX015", "EX016", "EX021", "EX014", "EX018", "EX019", "EX017", "EX003", "EX023"] }
                    }
                },
                { $lookup: { from: "referencevalues", localField: "patientorderitems.statusuid", foreignField: "_id", as: "status" } },
                { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        "status.valuecode": { $nin: ["ORDSTS4"] }
                    }
                }
            ],
            as: 'item'
        }
    },
    { $unwind: { path: "$item", preserveNullAndEmptyArrays: true } },
    {
        $addFields: {
            payor1: { $first: "$visitpayors.payoruid" }
        }
    },
    { $lookup: { from: "payors", localField: "payor1", foreignField: "_id", as: "payor1" } },
    { $unwind: { path: "$payor1", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "patientuid" } },
    { $unwind: { path: "$patientuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "users", localField: "item.patientorderitems.careprovideruid", foreignField: "_id", as: "doc" } },
    { $unwind: { path: "$doc", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "departments", localField: "item.orderdepartmentuid", foreignField: "_id", as: "departments" } },
    { $unwind: { path: "$departments", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientuid.titleuid", foreignField: "_id", as: "title" } },
    { $unwind: { path: "$title", preserveNullAndEmptyArrays: true } },
    {
        $project: {
            "firstname": "$patientuid.firstname",
            "lastname": { $ifNull: ["$patientuid.lastname", ""] },
            "mrn": "$patientuid.mrn",
            "dateofbirth": "$patientuid.dateofbirth",
            "visitid": "$visitid",
            "orderdate": "$item.orderdate",
            "ordername": "$item.patientorderitems.orderitemname",
            "payor": "$payor1.name",
            "doc": "$doc.name",
            "departments": "$departments.name",
            "title": "$title.valuedescription"
        }
    }

])