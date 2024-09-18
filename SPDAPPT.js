db.getCollection("patientvisits").aggregate([
    {
        $match: {
            "startdate": {
                $gte: ISODate("2024-08-28T00:00:00.000+07:00"),
                $lte: ISODate("2024-08-28T23:59:59.999+07:00")
            },
        }
    },
    {
        $lookup: {
            from: 'patientvisits',
            let: {
                visitid: "$_id",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$visitid', '$_id']
                        },
                    }
                },
                { $unwind: { path: '$visitjourneys', preserveNullAndEmptyArrays: true } },
                { $lookup: { from: "referencevalues", localField: "entypeuid", foreignField: "_id", as: "entypeuid" } },
                { $unwind: { path: "$entypeuid", preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        "entypeuid.valuecode": "ENTYPE1"
                    }
                },
                {
                    $sort: {
                        "visitjourneys.modifiedat": 1
                    }
                },
                { $lookup: { from: "referencevalues", localField: "visitjourneys.statusuid", foreignField: "_id", as: "status" } },
                { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
                { $lookup: { from: "departments", localField: "visitjourneys.departmentuid", foreignField: "_id", as: "department" } },
                { $unwind: { path: "$department", preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        "status.valuecode": "VSTSTS1"
                    }
                },
                {
                    $group: {
                        _id: "$_id",
                        department: { $last: "$department.name" },
                        code: { $last: "$department.code" }
                    }
                }
            ],
            as: 'department'
        }
    },
    { $unwind: { path: "$department", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "department.code": "OPD2SPC"
        }
    },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", foreignField: "_id", localField: "entypeuid", as: "encountertype" } },
    { $unwind: { path: "$encountertype", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$visitjourneys", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", foreignField: "_id", localField: "visitjourneys.statusuid", as: "vjstatusdoc" } },
    { $unwind: { path: "$vjstatusdoc", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "encountertype.valuecode": "ENTYPE1",
            "vjstatusdoc.valuecode": {
                $in: ["VSTSTS1", "VSTSTS2", "VSTSTS3", "VSTSTS4", "VSTSTS5", "VSTSTS6", "VSTSTS7"]
            },
        }
    },
    {
        $project: {
            _id: "$_id",
            title: "$HN.titleuid",
            fname: "$HN.firstname",
            lname: "$HN.lastname",
            HN: "$HN.mrn",
            jn: "$visitjourneys",
            "patientuid": "$HN._id",
            "optypeuid": "$optypeuid",
            "startdate": "$startdate",
            "visitid": "$visitid",
            "statusuid": "$vjstatusdoc._id",
            "doc": "$visitcareproviders",
            "registered": {
                "$cond": {
                    if: { $eq: ["$vjstatusdoc.valuecode", "VSTSTS1"] },
                    then: "$visitjourneys.modifiedat",
                    else: null
                }
            },
            "arrived": {
                "$cond": {
                    if: { $eq: ["$vjstatusdoc.valuecode", "VSTSTS2"] },
                    then: "$visitjourneys.modifiedat",
                    else: null
                }
            },
            "screeningcompleted": {
                "$cond": {
                    if: { $eq: ["$vjstatusdoc.valuecode", "VSTSTS3"] },
                    then: "$visitjourneys.modifiedat",
                    else: null
                }
            },
            "consultationstarted": {
                "$cond": {
                    if: { $eq: ["$vjstatusdoc.valuecode", "VSTSTS4"] },
                    then: "$visitjourneys.modifiedat",
                    else: null
                }
            },
            "consultationcompleted": {
                "$cond": {
                    if: { $eq: ["$vjstatusdoc.valuecode", "VSTSTS5"] },
                    then: "$visitjourneys.modifiedat",
                    else: null
                }
            },
            "medicaldischarge": {
                "$cond": {
                    if: { $eq: ["$vjstatusdoc.valuecode", "VSTSTS6"] },
                    then: "$visitjourneys.modifiedat",
                    else: null
                }
            },
            "financialdischarge": {
                "$cond": {
                    if: { $eq: ["$vjstatusdoc.valuecode", "VSTSTS7"] },
                    then: "$visitjourneys.modifiedat",
                    else: null
                }
            },
        }
    },
    { $lookup: { from: "patientorders", foreignField: "patientvisituid", localField: "_id", as: "patientorders" } },
    { $unwind: { path: "$patientorders", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$patientorders.patientorderitems", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$patientorders.patientorderitems.patientorderlogs", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", foreignField: "_id", localField: "patientorders.patientorderitems.patientorderlogs.statusuid", as: "patientorderitemsstatusuid" } },
    { $unwind: { path: "$patientorderitemsstatusuid", preserveNullAndEmptyArrays: true } },
    {
        $addFields: {
            "Dispensed": {
                "$cond": {
                    if: { $eq: ["$patientorderitemsstatusuid.valuecode", "ORDSTS22"] },
                    then: "$patientorders.patientorderitems.patientorderlogs.modifiedat",
                    else: null
                }
            },
        }
    },
    { $unwind: { path: "$doc", preserveNullAndEmptyArrays: true } },
    {
        $group: {
            "_id": "$_id",
            "title": { "$first": "$title" },
            fname: { "$first": "$fname" },
            lname: { "$first": "$lname" },
            departmentuid: { "$first": "$jn.departmentuid" },
            careprovideruid: { "$first": "$doc.careprovideruid" },
            HN: { "$first": "$HN" },
            "patientuid": { "$max": "$patientuid" },
            "visitdate": { "$first": "$startdate" },
            "visitid": { "$first": "$visitid" },
            "registered": { "$min": "$registered" },
            "arrived": { "$min": "$arrived" },
            "screeningcompleted": { "$min": "$screeningcompleted" },
            "consultationstarted": { "$min": "$consultationstarted" },
            "consultationcompleted": { "$max": "$consultationcompleted" },
            "medicaldischarge": { "$max": "$medicaldischarge" },
            "financialdischarge": { "$max": "$financialdischarge" },
            "Dispensed": { "$min": "$Dispensed" }
        }
    },
    {
       $sort:{
           visitdate:1
       } 
    },
    { $lookup: { from: "users", localField: "careprovideruid", foreignField: "_id", as: "careprovideruid" } },
    { $unwind: { path: "$careprovideruid", preserveNullAndEmptyArrays: true } },

    { $lookup: { from: "referencevalues", foreignField: "_id", localField: "title", as: "title" } },
    { $unwind: { path: "$title", preserveNullAndEmptyArrays: true } },
    {
        $project: {
            "_id": 1,
            "title": "$title.locallanguagedesc",
            fname: 1,
            lname: 1,
            departmentuid: "$departments.name",
            careprovideruid: "$careprovideruid.name",
            HN: 1,
            "patientuid": 1,
            "visitdate": 1,
            "visitid": 1,
            "registered": 1,
            "arrived": 1,
            "screeningcompleted": 1,
            "consultationstarted": 1,
            "consultationcompleted": 1,
            "medicaldischarge": 1,
            "financialdischarge": 1,
            "Dispensed": 1,
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
                        "patientorderitems.ordercattype": "LAB"
                    }
                },
                {
                    $group: {
                        _id: "$patientorderitems.ordercattype"
                    }
                }
            ],
            as: 'lab'
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
                        "patientorderitems.ordercattype": "RADIOLOGY"
                    }
                },
                {
                    $group: {
                        _id: "$patientorderitems.ordercattype"
                    }
                }
            ],
            as: 'xray'
        }
    },
    { $unwind: { path: "$lab", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$xray", preserveNullAndEmptyArrays: true } },
  {
    $addFields: {
      ssdate: {
        $dateFromParts: {
          year: { $year: "$visitdate" },
          month: { $month: "$visitdate" },
          day: { $dayOfMonth: "$visitdate" },
          hour: { $subtract: [{ $hour: "$visitdate" }, 12] },
          minute: { $minute: "$visitdate" },
          second: { $second: "$visitdate" },
          millisecond: { $millisecond: "$visitdate" }
        }
      },
      enddate: {
        $dateFromParts: {
          year: { $year: "$visitdate" },
          month: { $month: "$visitdate" },
          day: { $dayOfMonth: "$visitdate" },
          hour: { $add: [{ $hour: "$visitdate" }, 12] },
          minute: { $minute: "$visitdate" },
          second: { $second: "$visitdate" },
          millisecond: { $millisecond: "$visitdate" }
        }
      }
    }
  },
   {
        $lookup: {
            from: 'appointmentschedules',
            let: {
                sdate: "$ssdate",
                edate:"$enddate",
                ptuid:"$patientuid"
            },
            pipeline: [
            {
                    $match: {
                        $expr: {
                            $and: [
                                { $gte: ['$appointmentdate', '$$sdate'] },
                                { $lte: ['$appointmentdate', '$$edate'] },
                            ]
                        },
                        "departmentuid":ObjectId("63d9e0e0f458c20014126a3b")
                    }
                },
            { $unwind: { path: "$slots" } },
            { $lookup: { from: 'patients', localField: 'slots.patientuid', foreignField: '_id', as: 'patient' } },
            { $unwind: { path: '$patient', preserveNullAndEmptyArrays: true } },
                        {
                    $match: {
                        $expr:   { $eq: ['$$ptuid', '$slots.patientuid'] },
                    }
                },
      { $lookup: { from: "referencevalues", foreignField: "_id", localField: "slots.statusuid", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
    {
        $match:{
            "iscancelled" : false,
          "status.valuecode" : "BOKSTS6",
          
        }
    },            
    {
        $sort:{
            "slots.start":-1
        }
    },
            ],
            as: 'timeappt'
        }
    },
{
    $addFields:{
        time:{$last:"$timeappt.slots.start"}
    }
},


])
