db.getCollection("radiologyresults").aggregate([
    {
        $match: {
            "approvaldate": {
                $gte: ISODate("2024-07-29T00:00:00.000+07:00"),
                $lte: ISODate("2024-07-29T23:59:59.999+07:00")
            },
            "statusuid": ObjectId("579b0d24f3fd3fa90b83e33f")
        }
    },
    { $lookup: { from: "referencevalues", localField: "statusuid", foreignField: "_id", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },

    { $lookup: { from: 'users', localField: "radiologyresults.createdby", foreignField: '_id', as: "users" } },
    { $unwind: { path: '$users', preserveNullAndEmptyArrays: true } },
{
    $lookup: {
        from: 'patientorders',
        let: { 
            patientorderitemuid: '$patientorderitemuid', 
            patientvisituid: '$patientvisituid', 
            patientorderuid:"$patientorderuid"
            },
        pipeline: [
                    {
                $match: {
                    $expr: {
                        $eq: ['$$patientvisituid', '$patientvisituid'],
                             $eq: ['$$patientorderuid', '$_id'],
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
        ],
        as: 'PPPorderitem'
    }
},
    { $unwind: { path: '$PPPorderitem', preserveNullAndEmptyArrays: true } },
    
    { $lookup: { from: "patientvisits", localField: "patientvisituid", foreignField: "_id", as: "patientvisit" } },
    { $unwind: { path: "$patientvisit", preserveNullAndEmptyArrays: true } },
{
    $addFields:{
        "visitpayors":{$first:"$patientvisit.visitpayors.payoruid"}
    }
},
    { $lookup: { from: 'orderitems', localField: 'orderitemuid', foreignField: '_id', as: 'orderitem' } },
    { $unwind: { path: '$orderitem', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: "orderitem.defaultmodalityuid", foreignField: '_id', as: "defaultmodalityuid" } },
    { $unwind: { path: '$defaultmodalityuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'ordercategories', localField: 'orderitem.ordercatuid', foreignField: '_id', as: 'ordercatuid' } },
    { $unwind: { path: '$ordercatuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'ordercategories', localField: "orderitem.ordersubcatuid", foreignField: '_id', as: 'ordersubcatuid' } },
    { $unwind: { path: '$ordersubcatuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'departments', localField: 'PPPorderitem.userdepartmentuid', foreignField: '_id', as: 'userdepartmentuid' } },
    { $unwind: { path: '$userdepartmentuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'payors', localField: 'PPPorderitem.patientorderitems.patientvisitpayoruid', foreignField: '_id', as: 'payor' } },
    { $unwind: { path: '$payor', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'payors', localField: 'visitpayors', foreignField: '_id', as: 'payor2' } },
    { $unwind: { path: '$payor2', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: "patientvisit.optypeuid", foreignField: '_id', as: "optypeuid" } },
    { $unwind: { path: '$optypeuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'patients', localField: "patientuid", foreignField: '_id', as: 'patientuid' } },
    { $unwind: { path: '$patientuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'users', localField: 'PPPorderitem.patientorderitems.careprovideruid', foreignField: '_id', as: 'careprovideruid' } },
    { $unwind: { path: '$careprovideruid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'tariffs', localField: "PPPorderitem.patientorderitems.tariffuid", foreignField: '_id', as: 'tariffuid' } },
    { $unwind: { path: '$tariffuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: "PPPorderitem.entypeuid", foreignField: '_id', as: 'entypeuid' } },
    { $unwind: { path: '$entypeuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'billinggroups', localField: "tariffuid.billinggroupuid", foreignField: '_id', as: 'billinggroupuid' } },
    { $unwind: { path: '$billinggroupuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'billinggroups', localField: "tariffuid.billingsubgroupuid", foreignField: '_id', as: "billingsubgroupuid" } },
    { $unwind: { path: '$billingsubgroupuid', preserveNullAndEmptyArrays: true } },
       { $lookup: { from: 'referencevalues', localField: "radstsuid", foreignField: '_id', as: "radiologistuid" } },
    { $unwind: { path: '$radiologistuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'users', localField: "createdby", foreignField: '_id', as: "users" } },
    { $unwind: { path: '$users', preserveNullAndEmptyArrays: true } },
{
    $
}
])