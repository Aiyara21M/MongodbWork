db.getCollection("patientvisits").aggregate([
{
    $match:{
        visitid:"67-2I010936"
    }
},
    {
  $lookup: {
    from: 'patientchargecodes',
    let: {
        visitid:"$_id",
    },
    pipeline: [
      {
        $match: {
          $expr: {
            $eq: ['$$visitid', '$patientvisituid']
          },
        }
      },
     { $unwind: { path: "$chargecodes", preserveNullAndEmptyArrays: true } },
{
    $match:{
        "chargecodes.isbilled" : {$nin:[false]},
        "chargecodes.statusflag" : "A"
    }
},
{
    $match:{
        "chargecodes.chargedate":{
        $gte: ISODate("2024-08-28T00:00:00.000+0700"),
        $lte:  ISODate("2024-09-05T23:59:59.999+0700")
        }
    }
},
  { $lookup: { from: "billinggroups", localField: "chargecodes.billinggroupuid", foreignField: "_id", as: "billinggroupuid" } },
 { $unwind: { path: "$billinggroupuid", preserveNullAndEmptyArrays: true } },
{
  $addFields: {
    adjustedDate: {
      $cond: {
        if: { $gte: [{ $hour: "$chargecodes.chargedate" }, 17] }, 
        then: { $add: ["$chargecodes.chargedate", 25200000] }, 
        else: "$chargecodes.chargedate"
      }
    }
  }
},
{
    $addFields:{
              dategroup: {
        $dateToString: {
          format: "%d/%m/%Y", 
          date: "$adjustedDate" 
        }
      }
    }
},
{
    $sort:{
        "chargecodes.chargedate":1
    }
}
    ],
    as: 'detail'
  }
},
    { $unwind: { path: "$detail", preserveNullAndEmptyArrays: true } },
    {
        $addFields:{
            billinggroupuid:"$detail.billinggroupuid.name"
        }
    },
        {
    $sort:{
        billinggroupuid:1
    }
},
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
{
    $project:{
        itemname:"$detail.chargecodes.orderitemname",
        itemQTY:"$detail.chargecodes.quantity",
        resultPrice:"$detail.chargecodes.netamount",
        billgroup:"$detail.billinggroupuid.name",
       HN:"$HN.mrn",
       fname:"$HN.firstname",
       lname:"$HN.lastname",
       VN:"$visitid",
        orderdate:"$detail.chargecodes.chargedate",
        dategroup:"$detail.dategroup",
    }
},



])
