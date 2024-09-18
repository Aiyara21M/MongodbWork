db.getCollection("patientvisits").aggregate([
    {
        $match: {
            createdat: {
                $gte: ISODate("2024-07-01T00:00:00.000+07:00"),
                $lte: ISODate("2024-07-01T23:59:59.999+07:00")
            },
        }
    },
    {
        $addFields: {
            changeDepart: { $first: "$visitpayorslogs.reasontext" }
        }
    },
    { $lookup: { from: "departments", localField: "visitcareproviders.0.departmentuid", foreignField: "_id", as: "departC" } },
    { $unwind: { path: "$departC", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "patientuid" } },
    { $unwind: { path: "$patientuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "entypeuid", foreignField: "_id", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$visitjourneys", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "statusbeforelock.visitstatusuid", foreignField: "_id", as: "status2" } },
    { $unwind: { path: "$status2", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "visitjourneys.statusuid", foreignField: "_id", as: "status3" } },
    { $unwind: { path: "$status3", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "departments", localField: "visitjourneys.departmentuid", foreignField: "_id", as: "depart" } },
    { $unwind: { path: "$depart", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$visitcareproviders", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "users", localField: "visitcareproviders.careprovideruid", foreignField: "_id", as: "doc" } },
    { $unwind: { path: "$doc", preserveNullAndEmptyArrays: true } },
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
            departID: { "$first": "$departC.code" },
            status: { "$first": "$status.relatedvalue" },
            status2: { "$last": "$isreadmission" },
            comment: { "$max": "$visitjourneys.comments" },
            nationalid: { "$first": "$patientuid.nationalid" },
            departC: { "$first": "$departC.name" },
            departA: { "$first": "$depart.name" }
        }
    },
    {
        $match: {
            "departC": "HCCS-ศูนย์ตรวจสุขภาพประกันสังคม",
            status: "OPD",
            comment: { $ne: "Close Visit" },

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
            nationalid: 1,
            status: 1,
            totalbillamount: "$bill.totalbillamount",
            Billdate: "$bill.billdate",
            bill: {
                $cond: {
                    if: { $ne: ["$bill.iscancelled", true] },
                    then: { "$ifNull": ["$bill.sequencenumber", ""] },
                    else: ""
                }
            },
            departC: "$departC"
        },
    },
    {
        $group: {
            _id: "$visitid",
            createdat: { "$first": "$createdat" },
            visitid: { "$first": "$visitid" },
            HN: { "$first": "$HN" },
            fname: { "$first": "$fname" },
            lname: { "$first": "$lname" },
            dateofbirth: { "$first": "$dateofbirth" },
            isdobestimated: { "$first": "$isdobestimated" },
            departID: { "$max": "$departID" },
            status: { "$first": "$status" },
            bill: { "$last": "$bill" },
            nationalid: { "$first": "$nationalid" },
            totalbillamount: { "$last": "$totalbillamount" },
            departC: { $first: "$departC" },
            Billdate: { $last: "$Billdate" }
        }
    },
    {
        $match: {
            //           visitid:"67-2O327972",
            totalbillamount: { $nin: [null, ""] }
        }
    },
    {
        $sort: {
            visitid: 1,
            bill: 1
        }
    }

])
