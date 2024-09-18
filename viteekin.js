db.getCollection("patientvisits").aggregate([
    {
        $match: {
            "visitid": "67-2I006760"
        }
    },
    { $unwind: { path: "$bedoccupancy", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "bedoccupancy.isactive": true
        }
    },
    { $lookup: { from: "wards", localField: "bedoccupancy.warduid", foreignField: "_id", as: "warduid" } },
    { $unwind: { path: "$warduid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "beds", localField: "bedoccupancy.beduid", foreignField: "_id", as: "beduid" } },
    { $unwind: { path: "$beduid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "patientuid" } },
    { $unwind: { path: "$patientuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientuid.titleuid", foreignField: "_id", as: "titleuid" } },
    { $unwind: { path: "$titleuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientuid.genderuid", foreignField: "_id", as: "genderuid" } },
    { $unwind: { path: "$genderuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patientorders", localField: "_id", foreignField: "patientvisituid", as: "patientorders" } },
    { $unwind: { path: "$patientorders", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$patientorders.patientorderitems", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "medsadmins", localField: "patientorders._id", foreignField: "patientorderuid", as: "medsadmins" } },
    { $unwind: { path: "$medsadmins", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "medsadmins.statusuid", foreignField: "_id", as: "statusuid" } },
    { $unwind: { path: "$statusuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientorders.patientorderitems.quantityUOM", foreignField: "_id", as: "quantityuom" } },
    { $unwind: { path: "$quantityuom", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientorders.patientorderitems.statusuid", foreignField: "_id", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "frequencies", localField: "patientorders.patientorderitems.frequencyuid", foreignField: "_id", as: "frequency" } },
    { $unwind: { path: "$frequency", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientorders.patientorderitems.dosageUOM", foreignField: "_id", as: "dosageUOM" } },
    { $unwind: { path: "$dosageUOM", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientorders.patientorderitems.routeuid", foreignField: "_id", as: "route" } },
    { $unwind: { path: "$route", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$patientorders.patientorderitems.multiplefreqdetails", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "frequencies", localField: "patientorders.patientorderitems.multiplefreqdetails.frequencyuid", foreignField: "_id", as: "patientorders.patientorderitems.multiplefreqdetails.frequencyuid" } },
    { $unwind: { path: "$patientorders.patientorderitems.multiplefreqdetails.frequencyuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientorders.patientorderitems.multiplefreqdetails.routeuid", foreignField: "_id", as: "patientorders.patientorderitems.multiplefreqdetails.routeuid" } },
    { $unwind: { path: "$patientorders.patientorderitems.multiplefreqdetails.routeuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientorders.patientorderitems.dosageUOM", foreignField: "_id", as: "dosageUOM" } },
    { $unwind: { path: "$dosageUOM", preserveNullAndEmptyArrays: true } },
    { $match: { "patientorders.patientorderitems.ordercattype": "MEDICINE", } },
    { $match: { "$and": [{ "status.valuedescription": { $ne: "Cancelled" } }, { "status.valuedescription": { $ne: "Discontinued" } }] } },
    {
        $match: {
            "statusuid.valuedescription": "Administered"
        }
    },
    { $lookup: { from: "referencevalues", localField: "patientorders.ordertypeuid", foreignField: "_id", as: "ordertypeuid" } },
    { $unwind: { path: "$ordertypeuid", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientorders.patientorderitems.statusuid", foreignField: "_id", as: "statusuid1" } },
    { $unwind: { path: "$statusuid1", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "users", localField: "medsadmins.adminuseruid", foreignField: "_id", as: "adminuseruid" } },
    { $unwind: { path: "$adminuseruid", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "statusuid.valuecode": { $nin: ["ORDSTS4"] },
            "ordertypeuid.valuecode": { $in: ["ORDTYP2"] },
            //"ordertypeuid.valuecode": { $in: ["ORDTYP1"] },
            "patientorders.patientorderitems.chargecode": {
                $nin: ["PDOPTIG005GM", "PDAQUAG005GM", "PDSTERG003GM", "PDAQUAG082GM", "PENSSNG005ML", "PDSSE0G060ML ", "PIXYLJO0000", "PDXYLJO030GM",
                    "PEXYL1O001ML",
                    "PERACSG100ML",
                    "PDXYLOO050ML",
                    "PIXYL1O050ML",
                    "PIXYL2O050ML",
                    "PVSTERG001LT",
                    "PISTEVG010ML",
                    "PDSTEEG010ML",
                    "PDSTERG001LT",
                    "PECMOUG360ML",
                    "PESMW1G001ML" ,
                    "PEBMOUG180ML"]
            },
        }
    },
    {
        $project: {
            "visitid": "$visitid",
            "mrn": "$patientuid.mrn",
            "patientuid.firstname": "$patientuid.firstname",
            "patientuid.lastname": "$patientuid.lastname",
            "patientuid.isdobestimated":"$patientuid.isdobestimated",
            "patientuid.dateofbirth": "$patientuid.dateofbirth",
            "patientuid.titleuid": "$titleuid.valuedescription",
            "patientuid.genderuid": "$genderuid.valuedescription",
            "warduid": "$warduid.name",
            "beduid": "$beduid.name",
            "itemname": "$patientorders.patientorderitems.orderitemname",
            "quantity": "$patientorders.patientorderitems.quantity",
            "quantityuom": "$quantityuom.valuedescription",
            "status": "$status.valuedescription",
            "valuecode": "$status.valuecode",
            "frequency": "$frequency.locallangdesc",
            "route": "$route.locallanguagedesc",
            "dosage": { $ifNull: ["$patientorders.patientorderitems.dosage", 0] },
            "dosageUOM": { $ifNull: ["$dosageUOM.valuedescription", ""] },
            "admindate": "$medsadmins.admindate",
            "adminuseruid": "$adminuseruid.name",
            "administrationinstruction": { $ifNull: ["$patientorderitems.administrationinstruction", ""] },

        }
    },
    {
        $group: {
            "_id": "$itemname",
            "patientuid": { "$first": "$patientuid" },
            "warduid": { "$first": "$warduid" },
            "beduid": { "$first": "$beduid" },
            "visitid": { "$first": "$visitid" },
            "mrn": { "$first": "$mrn" },
            "drug": {
                "$push": {
                    "itemname": "$itemname",
                    "frequency": "$frequency",
                    "route": "$route",
                    "dosage": "$dosage",
                    "dosageUOM": "$dosageUOM",
                    "admindate": "$admindate",
                    "adminuseruid": "$adminuseruid",
                    "administrationinstruction": "$administrationinstruction",
                }
            },
        }
    },


{
    $group: {
        "_id": "$mrn", 
        "firstname": { "$first": "$patientuid.firstname" },
        "lastname": { "$first": "$patientuid.lastname" },
        "isdobestimated":{"$first":"$patientuid.isdobestimated"},
        "dateofbirth":{"$first":"$patientuid.dateofbirth"},
        "warduid": { "$first": "$warduid" },
        "beduid": { "$first": "$beduid" },
        "visitid": { "$first": "$visitid" },
        "datadrug": { "$push":"$drug"  }  
    }
},

])