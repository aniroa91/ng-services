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
        data: churnPercent
    })
    rs.push({
        name: "Churn Rate",
        type: 'spline',
        yAxis: 1,
        data: churnRate
    });

    return rs
}

function getJson2Column1Line(count_ct, churnPercent, count_call){
    var rs = []
    rs.push({
        name: "Count Contract",
        type: 'column',
        yAxis: 0,
        data: count_ct
    })
    rs.push({
        name: "Count Call",
        type: 'column',
        yAxis: 0,
        data: count_call
    })
    rs.push({
        name: "Churn Percent",
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