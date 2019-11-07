/* Author hoangnh44 */

/*comment Id by Page and id:
    [Overview*, Age*, Package*, ...]
*/

function insertComment(fields) {
    $.ajax({
        url: "/internet/pushComment",
        cache: false,
        data:  fields,
        type:"POST",
        success: function (dta) {
            if(dta == "Ok"){
                alert("Success")
            }
            else{
                alert("Error when insert comment")
            }
        }
    })
}

function parseTbChurnRate(data, arrMonth, id) {
    var location = data.name
    var dataChurn = data.data
    var mapMonth = arrMonth.reduce(function(map, obj) {
        map[obj] = "";
        return map;
    }, {});
    var tBody = ""
    for(var i=0; i< location.length; i++){
        Object.keys(mapMonth).forEach(function(key, value) {
            return mapMonth[key] = "";
        })
        var totalRate = 0, totalContract = 0, f1 = []
        tBody += "<tr>"
        tBody += "<td>"+location[i]+"</td>"
        for(var j=0; j< dataChurn.length; j++){
            if(dataChurn[j][0] == location[i]){
                if(dataChurn[j][1] == $('#monthCurr').val()){
                    totalRate += dataChurn[j][2]
                    totalContract += dataChurn[j][4]
                }
                if (mapMonth.hasOwnProperty(dataChurn[j][1])) {
                    mapMonth[dataChurn[j][1]] = dataChurn[j][3]
                }
                f1.push(dataChurn[j][3])
            }
        }
        tBody += "<td style='text-align: center'>"+totalContract.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')+"</td>"
        tBody += "<td style='text-align: center'>"+totalRate+"</td>"
        tBody += "<td style='text-align: center'><span class='sparkline"+id+"'>"+Object.values(mapMonth).join(",")+"</span></td>"
        tBody += "</tr>"
    }
    return tBody
}

function showMoreLess(event){
    if ($(event).parent().find('.dots').is(':visible')) {
        $(event).parent().find('.more').show()
        $(event).parent().find('.dots').hide()
        $(event).parent().find('i').removeClass('fa-angle-double-down').addClass('fa-angle-double-up')
    } else {
        $(event).parent().find('.dots').show()
        $(event).parent().find('.more').hide()
        $(event).parent().find('i').removeClass('fa-angle-double-up').addClass('fa-angle-double-down')
    }
}

function toogleTooltip(event){
    $('#indexChart').val($(event).attr('name'))
    var isVisibe = 0
    if ($(event).parent().find('.ddMore').is(':hidden')) {
        isVisibe = 1
    }
    $('.ddMore').hide()
    $('.dots').show()
    $('.more').hide()
    $('.tooltipSL').removeClass('fa-angle-double-up').addClass('fa-angle-double-down')
    if (isVisibe == 1) {
        $(event).parent().find('.ddMore').show()
    }
}

function updateCharts(charts, data){
    var seriesLength = charts.series.length;
    for(var i = seriesLength -1; i > -1; i--) {
        charts.series[i].remove();
    }
    for(var i=0; i< data.length; i++) {
        charts.addSeries(data[i]);
    }
}

function getJsonChurnMonth(churnRate, churnPercent){
    var rs = []
    rs.push({
        name: "Churn Percent",
        type: 'column',
        yAxis: 0,
        data: parseMulMetris(churnPercent)
    })
    rs.push({
        name: "Churn Rate",
        type: 'spline',
        yAxis: 1,
        data: churnRate
    });

    return rs
}

function getJsonChurnMonth2Col(churnRate, churnPercent){
    var rs = []
    rs.push({
        name: "Churn Percent",
        type: 'column',
        yAxis: 0,
        data: parseMulMetris(churnPercent)
    })
    rs.push({
        name: "Churn Rate",
        type: 'column',
        yAxis: 1,
        data: churnRate
    });

    return rs
}

function parseMulMetris(arrs) {
    var rs = []
    for(var i=0; i< arrs.length; i++) {
        rs.push({
            "y": arrs[i][0],
            "k": arrs[i][1]
        })
    }
    return rs
}

function getJson2Column1Line(count_ct, churnPercent, count_call, _type){
    var rs = []
    rs.push({
        name: "Count Contract",
        type: 'column',
        yAxis: 0,
        data: count_ct
    })
    rs.push({
        name: _type,
        type: 'column',
        yAxis: 0,
        data: count_call
    })
    rs.push({
        name: "Percent",
        type: 'line',
        yAxis: 1,
        data: churnPercent
    })

    return rs
}

var keyByValue = function(arrs, value) {
    var kArray = Object.keys(arrs);        // Creating array of keys
    var vArray = Object.values(arrs);      // Creating array of values
    var vIndex = vArray.indexOf(value);         // Finding value index

    return kArray[vIndex];                      // Returning key by value index
}

function getNameByIdAge(id){
    var age = "0-6"
    switch (id){
        case 6 :
            age = "0-6";break;
        case 12 :
            age = "06-12";break;
        case 18 :
            age = "12-18";break;
        case 24 :
            age = "18-24";break;
        case 30 :
            age = "24-30";break;
        case 36 :
            age = "30-36";break;
        case 42 :
            age = "36-42";break;
        case 48 :
            age = "42-48";break;
        case 54 :
            age = "48-54";break;
        case 60 :
            age = "54-60";break;
        case 66 :
            age = ">60";break;
    }
    return age
}

function getIdBynameAge(name){
    var age = 0
    switch (name){
        case "0-6" :
            age = 6;break;
        case "06-12" :
            age = 12;break;
        case "12-18" :
            age = 18;break;
        case "18-24" :
            age = 24;break;
        case "24-30" :
            age = 30;break;
        case "30-36" :
            age = 36;break;
        case "36-42" :
            age = 42;break;
        case "42-48" :
            age = 48;break;
        case "48-54" :
            age = 54;break;
        case "54-60" :
            age = 60;break;
        case ">60" :
            age = 66;break;
    }
    return age
}