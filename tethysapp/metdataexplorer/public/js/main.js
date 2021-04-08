////GLOBAL VARIABLES
//main.js
let firstlayeradded = false;
let shpfileAdded = false;
let URLpath = [];
let subsetURL = "";
let wmsURL = "";
let opendapURL = "";
let containerAttributes = false;
//timeseries.js
let chartdata = {};
//databases.js
let editing = false;
let urlInfoBox = false;
let fullArrayTimeseries = {};
let spatial_shape = false;
//Included with draw.js: drawnItems, shpLayer
//Included with map.js: mapObj, insetMapObj, basemapObj, layerControlObj

function updateFilepath() {
    $("#loading-modal").modal("show");
    if ($(this).attr("class") == "folder") {
        let newURL = $(this).attr("data-url");
        $("#url-input").val(newURL);
        getFoldersAndFiles(newURL);
    } else if ($(this).attr("class") == "file") {
        $('#name-in-form').attr('data-type', 'file');
        $("#url-input").val($(this).text().trim());
        $("#layer-display-container").css("display", "inline");
        $("#filetree-div").css("display", "none");
        $("#file-info-div").css("display", "flex");
        opendapURL = $(this).attr("data-opendap-url");
        subsetURL = $(this).attr("data-subset-url");
        wmsURL = $(this).attr("data-wms-url");
        if (containerAttributes === false) {
            let variablesAndFileMetadata = getVariablesAndFileMetadata();
            addVariables(variablesAndFileMetadata[0]);
            addFileMetadata(variablesAndFileMetadata[1]);
            let variableMetadataArray = variableMetadata();
            addVariableMetadata(variableMetadataArray);
            //addDimensions(dimensionsAndVariableMetadata[0]);
        } else {
            addContainerAttributesToUserInputItems();
        }
        updateWMSLayer();
    }
    $("#loading-modal").modal("hide");
}

function addVariables(variables) {
    console.log('Variables:')
    console.log(variables)
    let keys = Object.keys(variables);
    keys.sort();
    let html = "";
    for (let i = 0; i < keys.length; i++) {
        html += `<option data-dimensions="${variables[keys[i]]['dimensions']}" data-units="${variables[keys[i]]['units']}" data-color="${variables[keys[i]]['color']}">${keys[i]}</option>`;
    }
    $("#variable-input").empty().append(html);
    addDimensions($("#variable-input option:selected").attr('data-dimensions'));
}

function addDimensions(dimensions) {
    console.log(dimensions)
    dimensions = dimensions.split(',');
    let html = "";
    for (let i = 0; i < dimensions.length; i++) {
        html += `<option>${dimensions[i]}</option>`;
    }
    $("#time").empty().append(html);
    $("#time option:contains('time')").attr('selected', 'selected');
}

function addVariableMetadata(variableMetadata) {
    $("#var-metadata-div").empty().append(variableMetadata);
}

function addFileMetadata(fileMetadata) {
    $('#metadata-div').attr('data-description', fileMetadata);
    $('#metadata-div').empty().append(fileMetadata);
    $('#file-metadata-button').css("background-color", "#1600F0");
}

function updateWMSLayer() {
    if (firstlayeradded == true) {
        layerControlObj.removeLayer(dataLayerObj);
        mapObj.removeLayer(dataLayerObj);
    }
    dataLayerObj = data_layer();
    dataLayerObj.setOpacity($("#opacity-slider").val());
    layerControlObj.addOverlay(dataLayerObj, "Data Layer");
}

function removeWMSLayer() {
    layerControlObj.removeLayer(dataLayerObj);
    mapObj.removeLayer(dataLayerObj);
    firstlayeradded = false;
    $("#layer-display-container").css("display", "none");
}

/////////////////////Retrieve file data//////////////////////////////
function getVariablesAndFileMetadata() {
    //$("#loading-modal").modal("show");
    let variables = {};
    let fileMetadata = '';
    $.ajax({
        url: URL_getVariablesAndFileMetadata,
        data: {opendapURL: opendapURL},
        dataType: "json",
        contentType: "application/json",
        method: "GET",
        async: false,
        success: function (result) {
            variables = result["variables_sorted"];
            fileMetadata = result["file_metadata"];
        }
    })
    return [variables, fileMetadata];
}

function variableMetadata() {
    var variableMetadata = {};
    let variable = $("#variable-input").val();
    $.ajax({
        url: URL_getVariableMetadata,
        data: {
            variable: variable,
            opendapURL: opendapURL
        },
        dataType: "json",
        contentType: "application/json",
        method: "GET",
        async: false,
        success: function (result) {
            variableMetadata = result["variable_metadata"];
        }
    })
    return [variableMetadata];
}

function getFoldersAndFiles(url) {
    $('#name-in-form').attr('data-type', 'folder');
    $.ajax({
        url: URL_getFilesAndFolders,
        data: {'url': url},
        dataType: "json",
        contentType: "application/json",
        method: "GET",
        success: function (result) {
            var dataTree = result["dataTree"];
            if (dataTree == "Invalid URL") {
                alert(dataTree);
            } else {
                $("#filetree-div").css("display", "block");
                $("#file-info-div").css("display", "none");
                var correctURL = result["correct_url"];
                let html = ""
                for (var file in dataTree["files"]) {
                    html += `<div data-wms-url="${dataTree["files"][file]["WMS"]}" 
                            data-subset-url="${dataTree["files"][file]["NetcdfSubset"]}" 
                            data-opendap-url="${dataTree["files"][file]["OPENDAP"]}" 
                            class="file" onclick="updateFilepath.call(this)">
                            <img src="${pathToFileImage}" style="height: 20px; margin: 5px 10px 5px 10px">
                            <p style="padding: 5px 5px 5px 0px">${file}</p></div>`;
                }
                for (var folder in dataTree["folders"]) {
                    html += `<div data-url="${dataTree["folders"][folder]}" class="folder" 
                             onclick="updateFilepath.call(this)">
                             <img src="${pathToFolderImage}" style="height: 15px; margin: 5px 10px 5px 10px">
                             <p style="padding: 5px 5px 5px 0px">${folder}</p></div>`;
                }
                $("#filetree-div").empty().append(html);
                $("#url-input").val(correctURL);
                if (URLpath[URLpath.length - 1] !== correctURL) {
                    URLpath.push(correctURL);
                }
            }
            $("#loading-modal").modal("hide");
        }
    })
}

function getGeoserverLayerList() {
    var geoserverList = {};
    $.ajax({
        url: URL_listGeoserverLayers,
        dataType: 'json',
        contentType: "application/json",
        method: 'GET',
        async: false,
        success: function (result) {
            geoserverList = result['result'];
        }
    })
    return geoserverList;
}

function getLatestFile(url) {
    var latestFileURL = '';
    var fileName = '';
    $.ajax({
        url: URL_getLatestFiles,
        data: {'url': url},
        dataType: 'json',
        contentType: "application/json",
        method: 'GET',
        async: false,
        success: function (result) {
            latestFileURL = result['accessUrls'];
            fileName = result['fileName'];
        }
    })
    return {latestFileURL, fileName};
}


