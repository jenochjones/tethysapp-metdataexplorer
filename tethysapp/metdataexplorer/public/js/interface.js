//Get url database info
$('#go-input-button').click(function () {
    $('#loading-modal').modal('show');
    $('#metadata-div').empty();
    $('#var-metadata-div').empty();
    containerAttributes = false;
    if (firstlayeradded == true) {
        removeWMSLayer();
        firstlayeradded == false;
    }
    if (shpfileAdded == true) {
        removeGeojsonLayer();
        shpfileAdded == false;
    }
    getFoldersAndFiles($('#url-input').val());
});

$('.url-list-label').click(function () {
    $('#loading-modal').modal('show');
    $('#metadata-div').empty();
    $('#var-metadata-div').empty();
    containerAttributes = $(this).parents().closest('.url-list').data('thredds');
    console.log(containerAttributes);
    $('#metadata-div').empty().append(`<p>${containerAttributes['description'].replace(/\u000A/g, '<br>')}</p>`);
    removeGeojsonLayer();
    if (containerAttributes['spatial'] !== false && containerAttributes['type'] == 'file') {
        $('#get-full-array-button').prop('disabled', false).css('background-color', '#828cfa');
    } else {
        $('#get-full-array-button').prop('disabled', true).css('background-color', '#595959');
    }
    if (containerAttributes['spatial'] !== false) {
        configureBounds(containerAttributes['spatial']);
    }
    if (containerAttributes['type'] == 'file') {
        if (containerAttributes['timestamp'] == 'true') {
            var url_array = getLatestFile(containerAttributes['url']);
            opendapURL = url_array['latestFileURL']['OPENDAP'];
            wmsURL = url_array['latestFileURL']['WMS'];
            subsetURL = url_array['latestFileURL']['NetcdfSubset'];
        } else {
            var url_array = containerAttributes['url'].split(',');
            opendapURL = url_array['0'].slice(4);
            wmsURL = url_array['1'].slice(4);
            subsetURL = url_array['2'].slice(4);
        }
        addContainerAttributesToUserInputItems();
        updateWMSLayer();
        $('#loading-modal').modal('hide');
    } else {
        if (firstlayeradded == true) {
            removeWMSLayer();
        }
        getFoldersAndFiles(containerAttributes['url']);
    }
});

function addContainerAttributesToUserInputItems() {
    let variableMetadataString = false;
    if (containerAttributes['attributes'] == '' || containerAttributes['description'] == '') {
        let variablesAndFileMetadata = getVariablesAndFileMetadata();
        if (containerAttributes['attributes'] == '') {
            addVariables(variablesAndFileMetadata[0]);
        } else {
            addVariables(containerAttributes['attributes']);
        }
        if (containerAttributes['description'] == '') {
            addFileMetadata(variablesAndFileMetadata[1]);
        }
    } else {
        addVariables(containerAttributes['attributes']);
        $('#filetree-div').css('display', 'none');
        $('#file-info-div').css('display', 'flex');
        $('#layer-display-container').css('display', 'inline');
        console.log($('#variable-input option:selected').attr('data-color'))
        if($('#variable-input option:selected').attr('data-color') !== 'false') {
            $('#wmslayer-bounds').val($('#variable-input option:selected').attr('data-color'));
        } else {
            $('#wmslayer-bounds').val('0,25');
        }
    }
    if (variableMetadataString === false) {
        variableMetadataString = variableMetadata();
    }
    addVariableMetadata(variableMetadataString);
}

//Buttons for groups and metadata div
$('#file-metadata-button').click(function () {
    $('#var-metadata-div').css('display', 'none');
    $('#metadata-div').css('display', 'block');
    $('#file-metadata-button').css('background-color', '#1600F0');
    $('#var-metadata-button').css('background-color', '#828dcd');
});
$('#var-metadata-button').click(function () {
    $('#metadata-div').css('display', 'none');
    $('#var-metadata-div').css('display', 'block');
    $('#var-metadata-button').css('background-color', '#1600F0');
    $('#file-metadata-button').css('background-color', '#828dcd');
});

$('#demo-group-button').click(function () {
    $('#demo-group-button').attr('data-clicked', 'true');
    $('#user-group-button').attr('data-clicked', 'false');
    if ($('#upload-to-which-group').attr('data-admin') === 'True') {
        $('#upload-to-which-group').empty().append('Upload to: Demo Group');
    }
    $('#demo-group-button').css('background-color', '#1600F0');
    $('#user-group-button').css('background-color', '#828cfa');
    $('#demo-group-container').css('display', 'block');
    $('#user-group-container').css('display', 'none');
})

$('#user-group-button').click(function () {
    $('#demo-group-button').attr('data-clicked', 'false');
    $('#user-group-button').attr('data-clicked', 'true');
    if ($('#upload-to-which-group').attr('data-admin') === 'True') {
        $('#upload-to-which-group').empty().append('Upload to: User Group');
    }
    $('#user-group-button').css('background-color', '#1600F0');
    $('#demo-group-button').css('background-color', '#828cfa');
    $('#user-group-container').css('display', 'block');
    $('#demo-group-container').css('display', 'none');
})

$('#up-file').click(function () {
    if (URLpath.length !== 1) {
        let newURL = URLpath[URLpath.length - 2];
        $('#url-input').val(newURL);
        getFoldersAndFiles(newURL);
        URLpath.pop();
    }
})

$('#add-url').click(function () {
    if ($('#url-input').val() == '') {
        alert('Please enter a url to a Thredds Data Server.')
        return
    } else {
        $('#loading-modal').modal('show');
        containerAttributes = false;
        $('#main-body').css('display', 'none');
        $('#db-forms').css('display', 'block');
        $('#name-in-form').append($('#url-input').val());
        if ($('#name-in-form').attr('data-type') === 'file') {
            let html = '';
            let variables = {};
            $('#variable-input option').each(function () {
                variables[$(this).val()] = $(this).attr('data-dimensions');
            });
            for (let variable in variables) {
                let dimensionString = variables[variable];
                console.log(dimensionString)
                addAttribute(variable, dimensionString, '', '');
            }
            let description = $('#metadata-div').attr('data-description');
            $('#dimensions').append(html);
            $('#description-input').append(description);
        }
        urlInfoBox = true;
        $('#loading-modal').modal('hide');
    }
});

$('#variable-input').change(function () {
    let variableMetadataArray = variableMetadata();
    addVariableMetadata(variableMetadataArray);
    addDimensions($('#variable-input option:selected').attr('data-dimensions'));
    console.log($('#variable-input option:selected').attr('data-color'))
    if($('#variable-input option:selected').attr('data-color') !== 'false') {
        $('#wmslayer-bounds').val($('#variable-input option:selected').attr('data-color'));
    }
    updateWMSLayer();
});

$('#wmslayer-style').change(updateWMSLayer);
$('#wmslayer-bounds').change(updateWMSLayer);
$('#opacity-slider').change(function () {
    dataLayerObj.setOpacity($('#opacity-slider').val());
});

$('#link-geoserver').click(function () {
    let geoserverLayer = getGeoserverLayerList();
    let html = '';

    for (let workspace in geoserverLayer) {
        html += `<div style="width: 100%; height: auto;"><b style="padding-left: 0px;">${workspace}</b><br>`;
        for (let store in geoserverLayer[workspace]) {
            html += `<b style="padding-left: 20px">${store}</b><br>`;
            for (let name in geoserverLayer[workspace][store]) {
                html += `<div class="geoserver-layer-div" data-wfsURL="${geoserverLayer[workspace][store][name]}" ondblclick="setGeoserverWFS.call(this)"><b style="padding-left: 40px;">${name}</b></div>`;
            }
        }
    }
    $('#link-geoserver-inner-content').empty().append(html);
    $('#add-shape-resource-modal').modal('hide');
    $('#link-geoserver-modal').modal('show');
});

$('#get-full-array-button').click(getFullArray);

$('#hide-lower-content').click(function () {
    if($('#hide-lower-content').attr('data-hidden') == 'true') {
        $('#hide-lower-content').css('transform', 'rotate(0deg)');
        $('#lower-content').animate({height: "30%"});
        $('#map').animate({height: "70%"});
        $('#hide-lower-content').attr('data-hidden', 'false');
    } else {
        $('#hide-lower-content').css('transform', 'rotate(180deg)');
        $('#lower-content').animate({height: "0%"})
        $('#map').animate({height: "100%"});
        $('#hide-lower-content').attr('data-hidden', 'true');
    }
});

//////////////////////////Form Interface///////////////////////
$('#add-attribute-button').click(function () {
    addAttribute($('#add-attribute').val(), false, '', '');
    $('#add-attribute').val('');
});

$('#add-dimension-button').click(function () {
    let dimension = $('#add-dimension').val();
    $('#dimensions').append(`<option selected="selected">${dimension}</option>`);
    $('#add-dimension').val('');
})

$('#add-submit').click(function() {
    if ($('#title-input').val() == '') {
        alert('Please specify a name.');
        return
    } else if ($('#description-input').val() == '') {
        alert('Please include a description.');
        return
    } else {
        createDBArray();
        urlInfoBox = false;
    }
});

$('#add-cancel').click(function () {
    clearForm();
    $('.delete-url').attr('data-editing', 'false');
    $('#main-body').css('display', 'block');
    $('#db-forms').css('display', 'none');
    editing = false;
    urlInfoBox = false;
});

$('#select-all-button').click(function () {
    if ($('#select-all-button').attr('data-select') === 'true') {
        $('#select-all-button').attr('data-select', 'false');
        $('.attr-checkbox').each(function () {
            $(this).prop('checked', false);
        });
    } else {
        $('#select-all-button').attr('data-select', 'true');
        $('.attr-checkbox').each(function () {
            $(this).prop('checked', true);
        });
    }
});

$('#add-default-button').click(function (){
    let inputValue = $('#default-option-input').val();
    let selectValue = $('#default-option-select').val();
    if (selectValue == 'Time') {
        $('.time-dim-select option').each(function () {
            if ($(this).val().includes(inputValue)) {
                $(this).prop('selected','selected');
            }
        })
    } else if (selectValue == 'Lat') {
        $('.lat-dim-select option').each(function () {
            if ($(this).val().includes(inputValue)) {
                $(this).prop('selected','selected');
            }
        })
    } else if (selectValue == 'Lon') {
        $('.lon-dim-select option').each(function () {
            if ($(this).val().includes(inputValue)) {
                $(this).prop('selected','selected');
            }
        })
    } else if (selectValue == 'Color Range') {
        $('.var-color-select').each(function () {
            $('.var-color-select').val(inputValue);
        })
    } else if (selectValue == 'Units') {
        $('.var-unit-select').each(function () {
            $('.var-unit-select').val(inputValue);
        })
    }
})

$('#configure-for-latest').click(function () {
    $('#configure-for-latest-modal').modal('show');
    if (URLpath[URLpath.length - 1] == $('#name-in-form').text()) {
        $('#latest-url-input').val($('#name-in-form').text());
    } else {
        if ($('#latest-url-input').attr('data-url') == 'false') {
            $('#latest-url-input').val(`${URLpath[URLpath.length - 1].slice(0,-11)}${$('#name-in-form').text()}`);
        } else {
            $('#latest-url-input').val($('#latest-url-input').attr('data-url'));
        }
    }
})

$('#latest-url-confirm').click(function () {
    $('#latest-url-input').attr('data-url', $('#latest-url-input').val());
    $('#latest-input').val($('#latest-url-input').val());
    $('#configure-for-latest-modal').modal('hide');
})

$('#add-shape-resource-button').click(function (){
    $('#add-shape-resource-modal').modal('show');
})

function setGeoserverWFS() {
    let wfs = $(this).attr('data-wfsURL');
    if (wfs == false) {
        alert('Please enable wfs service on selected layer.');
    } else {
        $('#spatial-input').val(wfs);
        spatial_shape = wfs;
        $('#link-geoserver-modal').modal('hide');
        console.log(spatial_shape)
    }
}

function clearForm() {
    $('#name-in-form').empty();
    $('#title-input').val('');
    $('#epsg-input').val('');
    $('#latest-input').val('');
    $('#configure-for-latest').attr('data-select', 'false');
    $('#latest-url-input').attr('data-url', 'false');
    $('#latest-url-input').val('')
    $('#spatial-input').val('');
    spatial_shape = false;
    $('#description-input').val('');
    $('#attributes').empty();
}

/////////////////////////////////Geoserver
/*$('#crate-workspace-button').click(function () {
    let workspace = $('#workspace').val();
    let uri = $('#uri').val();
    $('#geoserver-modal').modal('hide');
    $.ajax({
        url: URL_createGeoserverWorkspace,
        data: {
            workspaceName: workspace,
            uri: uri,
        },
        dataType: "json",
        contentType: "application/json",
        method: "GET",
        async: false,
        success: function (result) {
        }
    })
})*/

/*$('#upload-shapefile-modal-button').click(function () {
    if ($('#which-workspace').val() == '' || $('#store-name').val() == '') {
        alert('Please specify a workspace and a store.')
    } else {
        $('#geoserver-modal').modal('hide');
        $('#uploadshp-modal').modal('show');
    }
})*/

$('#configure-geoserver-button').click(function () {
    $('#geoserver-modal').modal('show');
})

$('#upload-shape-resource').click(function () {
    $('#add-shape-resource-modal').modal('hide');
    $('#uploadshp-modal').modal('show');
})


////////////////////////TEST

$('#get-timeseries').click(function () {
    $.ajax({
        url: URL_GetTimeSeries,
        dataType: "json",
        contentType: "application/json",
        method: "GET",
        success: function (result) {
            let data = result['result'];
            formatForGraph(data);
        }
    })
})
