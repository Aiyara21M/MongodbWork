db.getCollection("patientvisits").aggregate([

    {
        $match: {
            createdat: {
                $gte: ISODate("2024-04-23T00:00:00.000+07:00"),
                $lte: ISODate("2024-04-23T23:59:59.999+07:00")
            },
        }
    },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "patientuid" } },
    { $unwind: { path: "$patientuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "entypeuid", foreignField: "_id", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$visitjourneys", preserveNullAndEmptyArrays: true } },
    {
        $group: {
            _id: "$_id",
            createdat: { "$first": "$createdat" },
            visitid: { "$first": "$visitid" },
            HN: { "$first": "$patientuid.mrn" },
            fname: { "$first": "$patientuid.firstname" },
            lname: { "$first": "$patientuid.lastname" },
            dateofbirth: { "$first": "$patientuid.dateofbirth" },
            isdobestimated: { "$first": "$patientuid.isdobestimated" },
            departID: { "$first": "$visitjourneys.departmentuid" },
            status: { "$first": "$status.relatedvalue" },
        }
    },
    { $lookup: { from: "departments", localField: "departID", foreignField: "_id", as: "depart" } },
    { $unwind: { path: "$depart", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            status: "OPD",
            "depart.code": "DEN"
        }
    },
    { $lookup: { from: "patientbills", localField: "_id", foreignField: "patientvisituid", as: "bill" } },
    { $unwind: { path: "$bill", preserveNullAndEmptyArrays: true } },
    {
        $project: {
            _id: 1,
            createdat: 1,
            visitid: 1,
            HN: 1,
            fname: 1,  
            lname: 1,
            dateofbirth: 1,
            isdobestimated: 1,
            departID: 1,
            status: 1,
            bill: "$bill.sequencenumber",
            depart: "$depart.code"
        }
    }
    ,
    {
        $sort: {
            visitid: 1,
            bill: 1
        }
    }

])