db.getCollection("patientvisits").aggregate([
    {
        $match: {
            visitid: "67-2I007265"
        }
    },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },

    { $unwind: { path: "$visitcareproviders", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "visitcareproviders.isprimarycareprovider": true
        }
    },
    { $lookup: { from: "dischargeprocesses", localField: "_id", foreignField: "patientvisituid", as: "dischargeprocesses" } },
    { $unwind: { path: "$dischargeprocesses", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "dischargeprocesses.statusuid", foreignField: "_id", as: "dischargeprocesses.statusuid" } },
    { $unwind: { path: "$dischargeprocesses.statusuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "dischargeprocesses.patientstatusuid", foreignField: "_id", as: "dischargeprocesses.patientstatusuid" } },
    { $unwind: { path: "$dischargeprocesses.patientstatusuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "deathrecords", localField: "_id", foreignField: "patientvisituid", as: "deathrecords" } },
    { $unwind: { path: "$deathrecords", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "departments", localField: "deathrecords.departmentuid", foreignField: "_id", as: "deathdepartment" } },
    { $unwind: { path: "$deathdepartment", preserveNullAndEmptyArrays: true } },

    { $lookup: { from: "users", localField: "visitcareproviders.careprovideruid", foreignField: "_id", as: "doctor" } },
    { $unwind: { path: "$doctor", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$bedoccupancy", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "beds", localField: "bedoccupancy.beduid", foreignField: "_id", as: "beds" } },
    { $unwind: { path: "$beds", preserveNullAndEmptyArrays: true } },
    {
        $group: {
            _id: "$_id",
            visitid: { $first: "$visitid" },
            HN: { $first: "$HN" },
            firstbed: { $first: "$beds" },
            lastbed: { $last: "$beds" },
            medicaldischargedate: { $first: "$medicaldischargedate" },
            visitpayors: { $first: "$visitpayors" },
            doctor: { "$first": "$doctor" },
            admitdate: { "$first": "$bedoccupancy.startdate" },
            DischStat: { "$last": "$dischargeprocesses.patientstatusuid.valuedescription" },
            DischTYPE: { "$last": "$dischargeprocesses.statusuid.valuedescription" },
            deathdatetime: { "$last": "$deathrecords.deathdatetime" },
            deathdepartment: { "$last": "$deathdepartment.name" },
        }
    },
    { $unwind: { path: "$visitpayors", preserveNullAndEmptyArrays: true } },
    {
        $addFields: {
            payor1: {
                $cond: {
                    if: { $eq: ["$visitpayors.orderofpreference", 1] },
                    then: "$visitpayors.payoruid",
                    else: null
                }
            }
        }
    },
    {
        $addFields: {
            payor2: {
                $cond: {
                    if: { $eq: ["$visitpayors.orderofpreference", 2] },
                    then: "$visitpayors.payoruid",
                    else: null
                }
            }
        }
    },
    {
        $addFields: {
            payor3: {
                $cond: {
                    if: { $eq: ["$visitpayors.orderofpreference", 3] },
                    then: "$visitpayors.payoruid",
                    else: null
                }
            }
        }
    },
    {
        $group: {
            _id: "$_id",
            visitid: { $first: "$visitid" },
            HN: { $first: "$HN" },
            firstbed: { $first: "$firstbed" },
            lastbed: { $last: "$lastbed" },
            admitdate: { $first: "$admitdate" },
            medicaldischargedate: { $first: "$medicaldischargedate" },
            payor1: { "$max": "$payor1" },
            payor2: { "$max": "$payor2" },
            payor3: { "$max": "$payor3" },
            doctor: { "$first": "$doctor" },
            DischStat: { "$last": "$DischStat" },
            DischTYPE: { "$last": "$DischTYPE" },
            deathdatetime: { "$last": "$deathdatetime" },
            deathdepartment: { "$last": "$deathdepartment" },
        }
    },
    { $lookup: { from: "payors", localField: "payor1", foreignField: "_id", as: "payors1" } },
    { $unwind: { path: "$payors1", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "payors", localField: "payor2", foreignField: "_id", as: "payors2" } },
    { $unwind: { path: "$payors2", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "payors", localField: "payor3", foreignField: "_id", as: "payors3" } },
    { $unwind: { path: "$payors3", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patientadditionaldetails", localField: "HN._id", foreignField: "patientuid", as: "patientadditionaldetails" } },
    { $unwind: { path: "$patientadditionaldetails", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$patientadditionaldetails.nokdetails", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientadditionaldetails.nokdetails.reltypeuid", foreignField: "_id", as: "type" } },
    { $unwind: { path: "$type", preserveNullAndEmptyArrays: true } },
    {
        $group: {
            _id: "$_id",
            visitid: { $first: "$visitid" },
            HN: { $first: "$HN" },
            firstbed: { $first: "$firstbed" },
            lastbed: { $last: "$lastbed" },
            admitdate: { $first: "$admitdate" },
            medicaldischargedate: { $first: "$medicaldischargedate" },
            payors1: { "$first": "$payors1" },
            payors2: { "$first": "$payors2" },
            payors3: { "$first": "$payors3" },
            patientadditionaldetails: { "$first": "$patientadditionaldetails.nokdetails" },
            type: { "$first": "$type" },
            doctor: { "$first": "$doctor" },
            DischStat: { "$last": "$DischStat" },
            DischTYPE: { "$last": "$DischTYPE" },
            deathdatetime: { "$last": "$deathdatetime" },
            deathdepartment: { "$last": "$deathdepartment" },
        }
    },
    { $lookup: { from: "referencevalues", localField: "HN.titleuid", foreignField: "_id", as: "HNtitle" } },
    { $unwind: { path: "$HNtitle", preserveNullAndEmptyArrays: true } },
    {
        "$project": {
            "_id": 1,
            "HN": "$HN.mrn",
            "fullname": {
                "$concat": [
                    { "$ifNull": ["$HNtitle.valuedescription", ""] },
                    " ",
                    {
                        "$cond": {
                            "if": {
                                "$or": [
                                    { "$gt": [{ "$indexOfBytes": ["$HN.firstname", "("] }, -1] },
                                    { "$gt": [{ "$indexOfBytes": ["$HN.firstname", ")"] }, -1] }
                                ]
                            },
                            "then": {
                                "$substrBytes": [
                                    "$HN.firstname",
                                    0,
                                    { "$subtract": [{ "$strLenBytes": "$HN.firstname" }, 11] }
                                ]
                            },
                            "else": "$HN.firstname"
                        }
                    },
                    " ",
                    { "$ifNull": ["$HN.lastname", ""] }
                ]
            },
            "dateofbirth": "$HN.dateofbirth",
            "isdobestimated": "$HN.isdobestimated",
            "nationalid": "$HN.nationalid",
            "address": "$HN.address.housenumber",
            "village": "$HN.address.village",
            "area": "$HN.address.area",
            "city": "$HN.address.city",
            "state": "$HN.address.state",
            "zipcode": "$HN.address.zipcode",
            "payors1": "$payors1.name",
            "payors2": "$payors2.name",
            "payors3": "$payors3.name",
            "firstbed": "$firstbed.name",
            "lastbed": "$lastbed.name",
            "codedoc": "$doctor.code",
            "namedoc": "$doctor.name",
            "admitdate": 1,
            "medicaldischargedate": 1,
            "noktype": "$type.locallanguagedesc",
            "nokname": "$patientadditionaldetails.nokname",
            "nokphone1": "$patientadditionaldetails.contact.mobilephone",
            "nokphone2": "$patientadditionaldetails.contact.workphone",
            "visitid": 1,
            "DischStat": 1,
            "DischTYPE": 1,
            "deathdatetime": 1
            "deathdepartment": 1
        }
    }



])