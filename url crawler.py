# source: https://medium.com/swlh/web-scraping-all-the-links-with-python-fbefa0472753
# https://pythonexamples.org/python-regex-check-if-string-ends-with-specific-word/
# https://pythonexamples.org/python-regex-check-if-string-starts-with-specific-word/
from bs4 import BeautifulSoup
import requests, re


def grab_urls():
    URLs = [
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823",
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823&page=2",
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823&page=3",
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823&page=4",
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823&page=5",
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823&page=6",
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823&page=7",
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823&page=8",
        "https://catalog.data.gov/dataset/?q=&sort=title_string+asc&tags=automated+shore+stations&ext_location=&ext_bbox=&ext_prev_extent=-150.46875%2C-80.17871349622823%2C151.875%2C80.17871349622823&page=9"]

    for URL in URLs:
        # we are looking for this URL: https://sccoos.org/thredds/dodsC/autoss/newport_pier-2007.nc
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")
        for a_href in soup.find_all("a", href=True):
            with open("urls.txt", "a") as linkfile:
                link = a_href["href"] + "\n"
                if re.search("^https://sccoos.org", link) and re.search(".nc$", link):
                    linkfile.write(link)

    archive_links = []


def sorting(filename):
    infile = open(filename)
    words = []
    for line in infile:
        temp = line.split()
        for i in temp:
            words.append(i)
    infile.close()
    words.sort()
    outfile = open("urls_sorted.txt", "w")
    lines_seen = []
    for i in words:
        if i not in lines_seen:
            outfile.writelines(i)
            outfile.writelines("\n")
            lines_seen.append(i)
    outfile.close()


def main():
    grab_urls()
    sorting("urls.txt")

main()