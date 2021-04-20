from django.http import JsonResponse
from siphon.catalog import TDSCatalog


def url_to_iterate_files(request):
    url = request.GET['url']
    print(url)
    access_urls, file_name = iterate_files(url)
    return JsonResponse({'accessUrls': access_urls, 'fileName': file_name})


def iterate_files(url):
    path_list = format_filepath(url)
    iteration = 1
    new_url = path_list[iteration - 1]
    while iteration < len(path_list) - 1:
        catalog = get_catalog(new_url)
        catalog_files = catalog.catalog_refs
        the_file = get_latest_file(path_list[iteration], catalog_files)
        print(the_file)
        new_url += the_file + '/'
        iteration += 1
    catalog = get_catalog(new_url)
    print(catalog)
    catalog_files = catalog.datasets
    parts_of_string = [path_list[iteration].split('#')[0], path_list[iteration].split('#')[2]]
    relevant_files = []
    for next_file in catalog_files:
        if next_file[:len(parts_of_string[0])] == parts_of_string[0] and \
                next_file[- len(parts_of_string[1]):] == parts_of_string[1]:
            relevant_files.append(next_file)
    next_file = get_latest_file(path_list[iteration], relevant_files)
    return catalog_files[next_file].access_urls, next_file


def get_latest_file(format_string, file_list):
    list_of_files = {}
    list_of_dates = []
    for file in file_list:
        date = check_dates(format_string, file)
        list_of_files[date] = file
    for num in list_of_files:
        list_of_dates.append(num)
    max_num = max(list_of_dates)
    final_file = list_of_files[max_num]
    return final_file


def get_catalog(url):
    ds = TDSCatalog(url + 'catalog.xml')
    return ds


def format_filepath(url):
    two_parts = url.split('#', 1)
    if not two_parts[0][-1] == '/':
        path_split = two_parts[0].split('/')
        two_parts[0] = two_parts[0][:-len(path_split[len(path_split) - 1])]
        two_parts[1] = path_split[len(path_split) - 1] + '#' + two_parts[1]
    else:
        two_parts[1] = '#' + two_parts[1]

    file_list = [two_parts[0]]
    for part in two_parts[1].split('/'):
        file_list.append(part)
    return file_list


def check_dates(format_string, file_string):
    full_number = ''
    number = []
    format_string = format_string.split('#')[1]
    if 'YYYY' in format_string:
        year = format_string.split('YYYY')
        marker_one, marker_two = get_markers(year)
        if marker_one is None and marker_two is None:
            number.append(file_string)
        else:
            number.append(file_string[marker_one:marker_two])
    if 'mm' in format_string:
        month = format_string.split('mm')
        marker_one, marker_two = get_markers(month)
        if marker_one is None and marker_two is None:
            number.append(file_string)
        else:
            number.append(file_string[marker_one:marker_two])
    if 'DD' in format_string:
        day = format_string.split('DD')
        marker_one, marker_two = get_markers(day)
        if marker_one is None and marker_two is None:
            number.append(file_string)
        else:
            number.append(file_string[marker_one:marker_two])
    if 'HH' in format_string:
        hour = format_string.split('HH')
        marker_one, marker_two = get_markers(hour)
        if marker_one is None and marker_two is None:
            number.append(file_string)
        else:
            number.append(file_string[marker_one:marker_two])
    if 'MM' in format_string:
        minute = format_string.split('MM')
        marker_one, marker_two = get_markers(minute)
        if marker_one is None and marker_two is None:
            number.append(file_string)
        else:
            number.append(file_string[marker_one:marker_two])
    if 'SS' in format_string:
        minute = format_string.split('SS')
        marker_one, marker_two = get_markers(minute)
        if marker_one is None and marker_two is None:
            number.append(file_string)
        else:
            number.append(file_string[marker_one:marker_two])

    for num in number:
        full_number += num
    return full_number


def get_markers(string_parts):
    if len(string_parts[0]) == 0:
        marker_one = None
    else:
        marker_one = len(string_parts[0]) - 1
    if len(string_parts[1]) == 0:
        marker_two = None
    else:
        marker_two = -len(string_parts[1])
    return marker_one, marker_two
