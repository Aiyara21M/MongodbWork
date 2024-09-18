db.getCollection("patientvisits").aggregate([

    { $lookup: { from: "referencevalues", localField: "entypeuid", foreignField: "_id", as: "entypeuid" } },
    { $unwind: { path: "$entypeuid", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "entypeuid.valuecode": "ENTYPE1"
        }
    },
    {
        $match: {
            visitid: "67-2O260142",
            medicaldischargedate: { $nin: [null, ""] }
        }
    },

    {
        $lookup: {
            from: 'patientbills',
            let: {
                visitid: "$_id",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$visitid', '$patientvisituid']
                        },
                        iscancelled: false
                    }
                },
                {
                    $project: {
                        net: "$totalbillamount",
                        billdate:"$billdate"
                    }
                }
            ],
            as: 'totalbill01'
        }
    },
    {
        $addFields: {
            sumtotal: { $sum: "$totalbill01.net" },
            billdate:{$last:"$totalbill01.billdate"}
        }
    },
    { $lookup: { from: "patientbills", localField: "_id", foreignField: "patientvisituid", as: "bill" } },
    { $unwind: { path: "$bill", preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "bill.iscancelled": { $nin: [true, null, ""] }
        }
    },
    { $unwind: { path: "$bill.patientbilleditems", preserveNullAndEmptyArrays: true } },
    {
        $lookup: {
            from: 'patientorders',
            let: {
                visitid: "$_id",
                patientorderitemuid: "$bill.patientbilleditems.patientorderitemuid"
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$visitid', '$patientvisituid']
                        }
                    }
                },
                { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
                {
                    $match: {
                        $expr: {
                            $eq: ['$$patientorderitemuid', '$patientorderitems._id']
                        }
                    }
                },
                {
                    $project: {
                        orderdate: "$orderdate",
                        tariff: "$patientorderitems.tariffuid",
                        qty: "$patientorderitems.quantity",
                     
                    }
                }
            ],
            as: 'order'
        }
    },
    {
        $addFields: {
            orderdate: { $last: "$order.orderdate" },
            tariff: { $last: "$order.tariff" },
            order: { $last: "$order.qty" },
        }
    },
        {
            $sort: {
                orderdate: 1
            }
        },
        {
            $addFields: {
                adjustedDate: {
                    $cond: {
                        if: { $gte: [{ $hour: "$orderdate" }, 17] },
                        then: { $add: ["$orderdate", 25200000] },
                        else: "$orderdate"
                    }
                }
            }
        },
        {
            $addFields: {
                dategroup: {
                    $dateToString: {
                        format: "%d/%m/%Y",
                        date: "$adjustedDate"
                    }
                }
            }
        },
        { $lookup: { from: "tariffs", localField: "tariff", foreignField: "_id", as: "tariff" } },
        { $unwind: { path: "$tariff", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "billinggroups", localField: "tariff.billinggroupuid", foreignField: "_id", as: "billinggroupuid" } },
        { $unwind: { path: "$billinggroupuid", preserveNullAndEmptyArrays: true } },
        {
            $sort: {
                "billinggroupuid.name": 1
            }
        },
        { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "patients" } },
        { $unwind: { path: "$patients", preserveNullAndEmptyArrays: true } },
        {
            $project: {
                itemname: "$bill.patientbilleditems.orderitemname",
                itemQTY: "$order",
                totalprice: "$bill.patientbilleditems.netamount",
                billgroup: "$billinggroupuid.name",
                HN: "$patients.mrn",
                fname: "$patients.firstname",
                lname: "$patients.lastname",
                VN: "$visitid",
                orderdate: "$orderdate",
                adjustedDate: "$adjustedDate",
                dategroup: "$dategroup",
    		    sumtotal:"$sumtotal",
                billdate:1
            }
        },
        {
            $sort: {
                dategroup: 1
            }
        }




])
