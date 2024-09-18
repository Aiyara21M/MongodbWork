db.getCollection("appointmentschedules").aggregate([

    {
        $match: {
            appointmentdate: {
                $gte: ISODate("2024-06-22T00:00:00.000+07:00"),
                $lte: ISODate("2024-06-24T23:59:59.999+07:00")
            }
        }
    },
     { $unwind: { path: "$slots" } },
    { $lookup: { from: 'patients', localField: 'slots.patientuid', foreignField: '_id', as: 'patient' } },
    { $unwind: { path: '$patient', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: 'slots.statusuid', foreignField: '_id', as: 'value' } },
    { $unwind: { path: '$value', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'users', localField: 'careprovideruid', foreignField: '_id', as: 'doc' } },
    { $unwind: { path: '$doc', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'departments', localField: 'departmentuid', foreignField: '_id', as: 'department' } },
    { $unwind: { path: '$department', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: 'patient.genderuid', foreignField: '_id', as: 'gender' } },
    { $unwind: { path: '$gender', preserveNullAndEmptyArrays: true } },
    {
        $project: {
            _id: "$_id",
            status: "$value.valuedescription",
            docid: "$doc._id",
            docname: "$doc.name",
            fname: "$patient.firstname",
            lname: "$patient.lastname",
            nationalid: "$patient.nationalid",
            dateofbirth: "$patient.dateofbirth",
            isdobestimated: "$patient.isdobestimated",
            HN: "$patient.mrn",
            phone: "$patient.contact.mobilephone",
            apmdate: "$slots.start",
            dpmName: "$department.name",
            dpmID: "$department._id",
            gender: "$gender.valuedescription",
            comment:"$slots.comments",
        }
    },
    {
        $sort:{
            apmdate:1
        }
    },
{
    "$addFields": {
        "Departreq": ""
    }
},
{
    "$addFields": {
               "department": {
            "$cond": {
                "if": { "$or": [{ "$eq": ["$Departreq", null] }, { "$eq": ["$Departreq", ""] }] },
                "then": ["000000000000000000000000"],
                "else": { "$split": ["$Departreq", ","] }
            }
        }
    }
},
    {
        $facet: {
            "trueCondition": [
                { $match: { Departreq: { $in: [null, ""] } } },
            ],
            "falseCondition": [
                {
                    $match: {
                        $expr: {
                            $in: ["$dpmID", { $map: { input: "$department", as: "dept", in: { $toObjectId: "$$dept" } } }]
                        }
                    }
                }
            ]
        }
    },
    {
        $addFields: {
            result: {
                $cond: {
                    if: { $eq: [{ $size: "$trueCondition" }, 0] }, 
                    then: "$falseCondition",
                    else: "$trueCondition"
                }
            }
        }
    },
    { $unset: ["falseCondition", "trueCondition"] },
    { $unwind: { path: "$result", preserveNullAndEmptyArrays: true } },
    {
        $project: {
            _id: "$result._id",
            status: "$result.status",
            docid: "$result.docid",
            docname: "$result.docname",
            fname: "$result.fname",
            lname:"$result.lname",
            nationalid: "$result.nationalid",
            dateofbirth: "$result.dateofbirth",
            isdobestimated: "$result.isdobestimated",
            HN: "$result.HN",
            phone: "$result.phone",
            apmdate: "$result.apmdate",
            dpmName: "$result.dpmName",
            dpmID: "$result.dpmID",
            gender: "$result.gender",
            comment:"$result.comment"
        }
    },
    {
        $sort:{
            docname:1
        }
    },
    {
    "$addFields": {
        "docReq":""
    }
},
    {
        $addFields: {
                           "careprovideruids": {
            "$cond": {
                "if": { "$or": [{ "$eq": ["$docReq", null] }, { "$eq": ["$docReq", ""] }] },
                "then": ["000000000000000000000000"],
                "else": { "$split": ["$docReq", ","] }
            }
        }
        }
    },
    {
        $facet: {
            "trueCondition": [
                { $match: { docReq: { $in: [null, ""] } } },
            ],
            "falseCondition": [
                {
                    $match: {
                        $expr: {
                            $in: ["$docid", { $map: { input: "$careprovideruids", as: "docR", in: { $toObjectId: "$$docR" } } }]
                        }
                    }
                }
            ]
        }
    },
    {
        $addFields: {
            result: {
                $cond: {
                    if: { $eq: [{ $size: "$trueCondition" }, 0] },
                    then: "$falseCondition",
                    else: "$trueCondition"
                }
            }
        }
    },
    { $unset: ["falseCondition", "trueCondition"] },
    { $unwind: { path: "$result", preserveNullAndEmptyArrays: true } },
    {
        $project: {
            _id: "$result._id",
            status: "$result.status",
            docname: "$result.docname",
            fname: "$result.fname",
          lname:"$result.lname",
            nationalid: "$result.nationalid",
            dateofbirth: "$result.dateofbirth",
            isdobestimated: "$result.isdobestimated",
            HN: "$result.HN",
            phone: "$result.phone",
            apmdate: "$result.apmdate",
            dpmName: "$result.dpmName",
            gender: "$result.gender",
 comment:"$result.comment"
        }
    },
    {
        $sort:{
            dpmName:1
        }
    },

])
