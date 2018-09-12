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

var keyByValue = function(arrs, value) {
    var kArray = Object.keys(arrs);        // Creating array of keys
    var vArray = Object.values(arrs);      // Creating array of values
    var vIndex = vArray.indexOf(value);         // Finding value index

    return kArray[vIndex];                      // Returning key by value index
}