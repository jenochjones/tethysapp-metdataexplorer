function getTimeseries(coord) {
    if (subsetURL == '') {
        alert('Please select a data layer.');
    } else {
        $('#loading-modal').modal('show');
        var maxlat = coord[0][2].lat;
        var maxlng = coord[0][2].lng;
        var minlat = coord[0][0].lat;
        var minlng = coord[0][0].lng;
        var vars = $('#variable-input').val();
        var time = $('#time').val();
        var subsetUrlFull = `${subsetURL}?var=${vars}&north=${maxlat}&west=${minlng}&east=${maxlng}&south=${minlat}&disableProjSubset=on&horizStride=1&temporal=all`;
        $.ajax({
            url: URL_getBoxValues,
            data: {
                subsetURL: subsetUrlFull,
                var: vars,
                time: time,
            },
            dataType: 'json',
            contentType: "application/json",
            method: 'GET',
            success: function (result) {
                var data = result['data'];
                if (data == false) {
                    $('#loading-modal').modal('hide');
                    alert('Invalid dimensions');
                } else {
                    drawGraph(data);
                    $('#loading-modal').modal('hide');
                    $('#timeseries-modal').modal('show');
                }
            },
        });
    }
}

function getFullArray() {
    $('#loading-modal').modal('show');
    $.ajax({
        url: URL_getFullArray,
        data: {
            containerName: containerAttributes['title'],
            containerGroup: containerAttributes['group'],
            //'containerAttributes': JSON.stringify(containerAttributes),
        },
        dataType: 'json',
        contentType: "application/json",
        method: 'GET',
        success: function (result) {
            let data = result['result'];
            let timeseries = {};
            let htmlVariables = '';
            let i = 1;
            for (let key in data) {
                timeseries[key] = JSON.parse(data[key])
                if (i == 1) {
                    htmlVariables += `<div class="timeseries-variable" data-variable="${key}" onclick="updateSelectedVariable.call(this)" data-selected="true" style="background-color: #4532fc;"><p style="color: white">${key}</p></div>`;
                } else {
                    htmlVariables += `<div class="timeseries-variable" data-variable="${key}" onclick="updateSelectedVariable.call(this)" data-selected="false"><p>${key}</p></div>`;
                }
                i += 1;
            }
            i = 1;
            let htmlFeatures = '';
            for (let feature in timeseries[Object.keys(timeseries)[0]]) {
                if (feature !== 'datetime') {
                    if (i == 1) {
                        htmlFeatures += `<div class="timeseries-features" onclick="updateSelectedFeature.call(this)" data-feature="${feature}" data-selected="true" style="background-color: #4532fc;"><p style="color: white">${feature}</p></div>`;
                    } else {
                        htmlFeatures += `<div class="timeseries-features" onclick="updateSelectedFeature.call(this)" data-feature="${feature}" data-selected="false"><p>${feature}</p></div>`;
                    }
                    i += 1;
                }
            }
            fullArrayTimeseries = timeseries;
            $('#timeseries-variable-div').empty().append(htmlVariables);
            $('#timeseries-feature-div').empty().append(htmlFeatures);
            $('#full-array-modal').modal('show');
            $('#loading-modal').modal('hide');
            drawGraphTwo();
        },
    });
}

function updateSelectedVariable() {
    $('.timeseries-variable').each(function () {
        $(this).attr('data-selected', 'false').css('background-color', 'white');
        $(this).find('p').css('color', '#666');
    })
    $(this).attr('data-selected', 'true').css('color', 'white').css('background-color', '#4532fc');
    $(this).find('p').css('color', 'white');
    drawGraphTwo();
}

function updateSelectedFeature() {
    $('.timeseries-features').each(function () {
        $(this).attr('data-selected', 'false').css('background-color', 'white');
        $(this).find('p').css('color', '#666');
    })
    $(this).attr('data-selected', 'true').css('color', 'white').css('background-color', '#4532fc');
    $(this).find('p').css('color', 'white');
    drawGraphTwo();
}

function drawGraphTwo() {
    let timeseriesVariable = false;
    let timeseriesFeature = false;
    $('.timeseries-variable').each(function () {
        if ($(this).attr('data-selected') == 'true') {
            timeseriesVariable = $(this).attr('data-variable');
        }
    })
    $('.timeseries-features').each(function () {
        if ($(this).attr('data-selected') == 'true') {
            timeseriesFeature = $(this).attr('data-feature');
        }
    })
    let series = {};
    console.log('timeseries')
    console.log(fullArrayTimeseries[timeseriesVariable])
    series['timeseries'] = fullArrayTimeseries[timeseriesVariable]['datetime'];
    series['mean'] = fullArrayTimeseries[timeseriesVariable][timeseriesFeature];
    let x = [];
    let y = [];
    for (let i = 0; i < Object.keys(series['timeseries']).length; i++) {
        x.push(series['timeseries'][i]);
        y.push(series['mean'][i]);
    }
    let variable = $('#variable-input').val();
    let layout = {
        title: 'Mean of ' + variable,
        xaxis: {title: 'Time', type: 'datetime'},
        yaxis: {title: 'Amount'}
    };
    let values = {
        x: x,
        y: y,
        mode: 'lines+markers',
        type: 'scatter'
    };
    Plotly.newPlot('chart-two', [values], layout);
    let chart = $("#chart-two");
    Plotly.Plots.resize(chart[0]);
}

function drawGraph(data) {
    var series = $.parseJSON(data);
    let variable = $('#variable-input').val();
    fullArrayTimeseries = {};
    fullArrayTimeseries[variable] = {
        datetime: series['timeseries'],
        mean: series['mean'],
    }
    console.log(series)
    let x = [];
    let y = [];
    for (var i = 0; i < Object.keys(series['timeseries']).length; i++) {
        x.push(series['timeseries'][i]);
        y.push(series['mean'][i]);
    }
    let layout = {
        title: 'Mean of ' + variable,
        xaxis: {title: 'Time', type: 'datetime'},
        yaxis: {title: 'Amount'}
    };
    let values = {
        x: x,
        y: y,
        mode: 'lines+markers',
        type: 'scatter'
    };
    Plotly.newPlot('chart', [values], layout);
    let chart = $("#chart");
    Plotly.Plots.resize(chart[0]);
}

// Add function to save chart to CSV

function chartToCSV() {
    function format(arrays, variable) {
        let headers = ['datetime'];
        let row = [];
        let rows = '';
        let csv = '';
        for (let entry in arrays[variable]) {
            if (entry !== 'datetime') {
                if (entry.includes(',')) {
                    entry = entry.split(',').join('","');
                }
                headers.push(entry);
            }
        }
        for (let i = 0; i < Object.keys(arrays[variable]['datetime']).length; i++) {
            for (let r = 0; r < headers.length; r++) {
                row.push(arrays[variable][headers[r]][i]);
            }
            rows += row.join(',') + '\n';
            row = [];
        }
        csv = headers.join(',') + '\n' + rows;
        return [csv, variable]
    }

    if (chartdata === {}) {
        alert('There is no data in the chart. Please plot some data first.');
        return
    }
    for (let variable in fullArrayTimeseries) {
        var data = format(fullArrayTimeseries, variable);
    }
    let csv = "data:text/csv;charset=utf-8," + data[0];
    let link = document.createElement('a');
    link.setAttribute('href', encodeURI(csv));
    link.setAttribute('target', '_blank');
    link.setAttribute('download', data[1] + '_timeseries.csv');
    document.body.appendChild(link);
    link.click();
    $("#a").remove()
}

// WHEN YOU CLICK ON THE DOWNLOAD BUTTON- RUN THE DOWNLOAD CSV FUNCTION
$("#download-csv").click(chartToCSV);
$("#download-csv-two").click(chartToCSV);

//TODO fix add folder to catalog