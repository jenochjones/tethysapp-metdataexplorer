function addAttribute(attribute, dimensionString, units, color) {
    let options = '';
    let dimOptions;

    if (dimensionString == false) {
        dimOptions = `<div style="width: 100%; height: auto; display: flex; flex-direction: row;"> 
                            <b style="margin-right: 12px">Time</b>
                            <input class="time-dim-select" style="width: 100px; height: 20px">
                            <b>Lat</b>
                            <input class="lat-dim-select" style="width: 100px; height: 20px">
                            <b>Lon</b>
                            <input class="lon-dim-select" style="width: 100px; height: 20px">
                          </div>`
    } else {
        let dimensionList = dimensionString.split(',');
        for(let i = 0; i < dimensionList.length; i++) {
            options += `<option>${dimensionList[i]}</option>`;
        }
        dimOptions = `<div style="width: 100%; height: auto; display: flex; flex-direction: row;"> 
                            <b style="margin-right: 12px">Time</b>
                            <select class="time-dim-select" style="width: 100px">${options}</select>
                            <b>Lat</b>
                            <select class="lat-dim-select" style="width: 100px">${options}</select>
                            <b>Lon</b>
                            <select class="lon-dim-select" style="width: 100px">${options}</select>
                          </div>`
    }

    let count = $('.attr-checkbox').length;
    let html = `<div class="attr-div">
                    <div>
                        <input type="checkbox" class="attr-checkbox" checked id="attribute-${count}" style="margin: 0px 10px 0px 20px;">
                        <label for="attribute-${count}" style="margin: 0px 0px 20px 0px;">${attribute}</label>
                    </div>
                    ${dimOptions}
                    <div style="width: 100%; height: auto; display: flex; flex-direction: row;"> 
                        <b>Units</b>
                        <input class="var-unit-select" style="height: 20px; width: 100px" value="${ units }">
                        <b style="margin-left: 111px;">Color Range</b>
                        <input class="var-color-select" style="height: 20px; width: 100px" value="${ color }">
                    </div>
                </div>`;
    $('#attributes').append(html);
}

function createDBArray() {
    if (editing === true) {
        $('.delete-url[data-editing="true"]').trigger( "click" );
        var url = containerAttributes['url'];
    } else {
        if ($('#latest-url-input').attr('data-url') !== 'false') {
            var url = $('#latest-url-input').attr('data-url');
            var timestamp = 'true';
        } else if ($('#name-in-form').attr('data-type') == 'file') {
            var url = `opd:${opendapURL},wms:${wmsURL},sub:${subsetURL},ful:${URLpath[URLpath.length - 1].slice(0,-11)}${$('#name-in-form').text()}`;
            var timestamp = 'false';
        } else {
            var url = $('#url-input').val();
            var timestamp = 'false';
        }
    }
    let attr = {};
    $('.attr-checkbox').each(function () {
        if (this.checked) {
            let time = '';
            let lat = '';
            let lon = '';
            let units = '';
            let color = '';
            if ($(this).parents('.attr-div').find('.time-dim-select').val() == '') {
                time = false;
            } else {
                time = $(this).parents('.attr-div').find('.time-dim-select').val();
            }
            if ($(this).parents('.attr-div').find('.lat-dim-select').val() == '') {
                lat = false;
            } else {
                lat = $(this).parents('.attr-div').find('.lat-dim-select').val();
            }
            if ($(this).parents('.attr-div').find('.lon-dim-select').val() == '') {
                lon = false;
            } else {
                lon = $(this).parents('.attr-div').find('.lon-dim-select').val();
            }
            if ($(this).parents('.attr-div').find('.var-unit-select').val() == '') {
                units = false;
            } else {
                units = $(this).parents('.attr-div').find('.var-unit-select').val();
            }
            if ($(this).parents('.attr-div').find('.var-color-select').val() == '') {
                color = false;
            } else {
                color = $(this).parents('.attr-div').find('.var-color-select').val();
            }
            attr[$(this).next('label').text()] = {
                dimensions: `${time},${lat},${lon}`,
                units: units,
                color: color,
            }
        }
    })
    if ($('#upload-to-which-group').attr('data-admin') === 'True') {
        if ($('#demo-group-button').attr('data-clicked') === 'true') {
            var group = 'Demo Group';
            var groupID = 'demo-group-container';
        } else {
            var group = 'User Group';
            var groupID = 'user-group-container';
        }
    } else {
        var group = 'User Group';
        var groupID = 'user-group-container';
    }
    if ($('#epsg-input').val() == '') {
        var epsg = false;
    } else {
        var epsg = $('#epsg-input').val();
    }
    if (spatial_shape == false) {
        if ($('#spatial-input').val() == '') {
            spatial_shape = false;
        } else {
            spatial_shape = $('#spatial-input').val();
        }
    }
    let cron = `${$('#cron-number-input').val()},${$('#cron-date-select').val()},${$('#cron-start-select').val()}`;
    let databaseInfo = {
        type: $('#name-in-form').attr('data-type'),
        group: group,
        title: $('#title-input').val(),
        url: url,
        epsg: epsg,
        spatial: spatial_shape,
        description: $('#description-input').val(),
        attributes: attr,
        timestamp: timestamp,
        cron: cron,
    };
    console.log(databaseInfo)
    $.ajax({
        url: URL_updateDB,
        dataType: 'json',
        data: {data: JSON.stringify(databaseInfo)},
        contentType: "application/json",
        method: 'GET',
        success: function () {
            $('#main-body').css('display', 'block');
            $('#db-forms').css('display', 'none');
            let clone = $('#main-url').clone(true).attr('id', 'cloned').css('display', 'flex').attr('data-thredds', JSON.stringify(databaseInfo));
            $('#' + groupID + '').find('.group-container').append(clone);
            $('#cloned').find('.url-list-label').append(`<h4>${$('#title-input').val()}</h4>`);
            $('#cloned').removeAttr('id');
            clearForm();
            containerAttributes = false;
            editing = false;
        }
    })
}
//ToDo fix delete all
function deleteDB () {
    if ($(this).attr('class') == 'delete-url img-button') {
        var all = false;
        var thredds_array = $(this).parents().closest('.url-list').data('thredds');
        console.log(thredds_array)
        var dbInfo = {
            'all': all,
            'title': thredds_array['title'],
            'group': thredds_array['group'],
        }
    } else {
        var all = true;
        var dbInfo = {
            'all': all,
            'title': 'all servers',
            'group': 'all groups',
        }
    }
    if (typeof(thredds_array['spatial']) === 'string') {
        if (thredds_array['spatial'].slice(0, 4) == 'http') {
            dbInfo['spatial'] = false;
        } else {
            dbInfo['spatial'] = thredds_array['spatial'];
        }
    } else {
        dbInfo['spatial'] = false;
    }
    if (editing === false) {
        var con = confirm('Are you sure you want to delete ' + dbInfo['title'] + '? This action cannot be undone.');
    } else {
        var con = true;
    }
    if (con == true) {
        if (dbInfo['all']) {
            $(this).parents().closest('span').children().closest('.group-container').empty();
        } else {
            $(this).parents().closest('.url-list').remove();
        }
        console.log(dbInfo)
        $.ajax({
            url: URL_deleteContainer,
            data: dbInfo,
            dataType: 'json',
            contentType: 'application/json',
            method: 'GET',
        })
    }
}

//TODO fix editDB
function editDB () {
    $("#loading-modal").modal("show");
    editing = true;
    clearForm();
    $('.delete-url').attr('data-editing', 'false');
    $(this).siblings('.delete-url').attr('data-editing', 'true');
    containerAttributes = $(this).parents().closest('.url-list').data('thredds');
    $('#name-in-form').attr('data-type', containerAttributes['type']);
    $('#title-input').val(containerAttributes['title']);
    console.log(containerAttributes['epsg'])
    if (containerAttributes['timestamp'] == 'true') {
        $('#latest-input').val(containerAttributes['url']);
    } else {
        $('#latest-url-input').val(containerAttributes['url']);
    }
    if (containerAttributes['spatial'] !== false) {
        $('#spatial-input').val(containerAttributes['spatial']);
        spatial_shape = containerAttributes['spatial'];
    }
    if (containerAttributes['epsg'] !== 'false') {
        $('#epsg-input').val(containerAttributes['epsg']);
    }
    $('#description-input').val(containerAttributes['description']);
    $('#latest-url-input').val(containerAttributes['timestamp']);
    let attributes = containerAttributes['attributes'];
    for (let attribute in attributes) {
        addAttribute(attribute, attributes[attribute]['dimensions'], attributes[attribute]['units'], attributes[attribute]['color']);
    }
    $('#main-body').css('display', 'none');
    $('#db-forms').css('display', 'block');
    urlInfoBox = true;
    $("#loading-modal").modal("hide");
}

$('#info-box-exit').click(function () {
    urlInfoBox = false;
})

function infoDB() {
    if ($(this).attr('class') == 'info-url img-button') {
        containerAttributes = $(this).parents().closest('.url-list').data('thredds');
        let spatial = '';
        if (containerAttributes['spatial'] == false){
            spatial = 'None';
        } else {
            spatial = containerAttributes['spatial'];
        }
        if (containerAttributes['timestamp'] == 'true') {
            let html = `<b>URL Formatted for Latest:</b><br><p>${containerAttributes['url']}</p>
                        <b>Spatial:</b><br><p>${spatial}</p>
                        <b>Description:</b><br><p>${containerAttributes['description']}</p>`
            $('#info-title').text(containerAttributes['title']);
            $('#main-container-info').empty().append(html);
            $('#url-info-modal').modal("show");
        } else if (containerAttributes['type'] == 'file') {
            let urls = containerAttributes['url'].split(',');
            let html = `<b>Thredds URL:</b><br><p>${urls[3].slice(4)}</p>
                        <b>URL Access Points:</b><br><p>Opendap: ${urls[0].slice(4)}<br>WMS: ${urls[1].slice(4)}
                        <br>Subset: ${urls[2].slice(4)}</p>
                        <b>Spatial:</b><br><p>${spatial}</p>
                        <b>Description:</b><br><p>${containerAttributes['description']}</p>`
            $('#info-title').text(containerAttributes['title']);
            $('#main-container-info').empty().append(html);
            $('#url-info-modal').modal("show");
        } else {
            let html = `<b>URL:</b><br><p>${containerAttributes['url']}</p>
                        <b>Spatial:</b><br><p>${spatial}</p>
                        <b>Description:</b><br><p>${containerAttributes['description']}</p>`
            $('#info-title').text(containerAttributes['title']);
            $('#main-container-info').empty().append(html);
            $('#url-info-modal').modal("show");
        }
        containerAttributes = false;
    }
}


