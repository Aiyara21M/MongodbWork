db.getCollection("observations").aggregate([
  { $match: { "statusflag": "A", "isreusedfromothermodules": false, "patientvisituid" :   ObjectId("66a2f1129be42c000182f1db")} },
            { $sort: { "observationdate": 1 } },
            { $unwind: { path: "$observationvalues", preserveNullAndEmptyArrays: true } },
            {
                $match: {
                    "observationvalues.name": {
                        $in: ["Temperature", "Systolic BP", "Diastolic BP", "Pulse", "Respiration Rate", "Weight", "Height", "Head Circumference", "BMI", "O2 saturation level", "Pain Score"]
                    }
                }
            },
               {
        $group: {
            "_id": { "name": "$observationvalues.name" },
            "uid": { "$first": "$_id" },
            "name": { "$first": "$observationvalues.name" },
            "values": {
                $push: {
                    "shorttext": "$observationvalues.shorttext",
                    "resultvalue": "$observationvalues.resultvalue"
                }
            },
                      "uomuid": { "$first": "$observationvalues.uomuid" }
        }
    },
                            { $lookup: { from: "referencevalues", localField: "uomuid", foreignField: "_id", as: "uom" } },
            { $unwind: { path: "$uom", preserveNullAndEmptyArrays: true } },
    {
        $addFields: {
            filteredValues: {
                $filter: {
                    input: "$values",
                    as: "item",
                    cond: { $ne: ["$$item.resultvalue", null] }
                }
            }
        }
    },
    {
        $addFields: {
            reversedValues: { $reverseArray: "$filteredValues" }
        }
    },
    {
        $addFields: {
            value: {
                $arrayElemAt: ["$reversedValues.resultvalue", 0]
            }
        }
    },
{
    $addFields:{
                    "temperature": {
                        "$cond": {
                            if: { $eq: ["$name", "Temperature"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "systolicbp": {
                        "$cond": {
                            if: { $eq: ["$name", "Systolic BP"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "diastolicbp": {
                        "$cond": {
                            if: { $eq: ["$name", "Diastolic BP"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "pulse": {
                        "$cond": {
                            if: { $eq: ["$name", "Pulse"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "respirationrate": {
                        "$cond": {
                            if: { $eq: ["$name", "Respiration Rate"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "weight": {
                        "$cond": {
                            if: { $eq: ["$name", "Weight"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "height": {
                        "$cond": {
                            if: { $eq: ["$name", "Height"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "headcircumference": {
                        "$cond": {
                            if: { $eq: ["$name", "Head Circumference"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "bmi": {
                        "$cond": {
                            if: { $eq: ["$name", "BMI"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "o2saturationlevel": {
                        "$cond": {
                            if: { $eq: ["$name", "O2 saturation level"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    },
                    "painscore": {
                        "$cond": {
                            if: { $eq: ["$name", "Pain Score"] },
                            then: { $concat: ["$value", " ", "$uom.valuedescription"] },
                            else: null
                        }
                    }
    }
},
            {
                $group: {
                    "_id": { "uid": "$uid" },
                    "temperature": { "$max": "$temperature" },
                    "systolicbp": { "$max": "$systolicbp" },
                    "diastolicbp": { "$max": "$diastolicbp" },
                    "pulse": { "$max": "$pulse" },
                    "respirationrate": { "$max": "$respirationrate" },
                    "weight": { "$max": "$weight" },
                    "height": { "$max": "$height" },
                    "headcircumference": { "$max": "$headcircumference" },
                    "bmi": { "$max": "$bmi" },
                    "o2saturationlevel": { "$max": "$o2saturationlevel" },
                    "painscore": { "$max": "$painscore" }
                }
            }




])
