from playwright.sync_api import sync_playwright
import configparser
import json
import csv
import os
from os import listdir
from os.path import isfile, join
import time
import random
import pandas as pd
import numpy as np
import logging


PROXY = {
    "server": "http://zproxy.lum-superproxy.io:22225",
    "user": "lum-customer-c_4767f99e-zone-zone1_us",
    "password": "giynzth1ngev",
}


# permit_number= "BCP2021-040903"

BASE_URL = (
    "https://epermit.myclearwater.com/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=Home"
)

index = 1


def get_data(path=""):
    """Get links from the CSV file

    Parameters:
    path to csv file

    Returns:
    void

    """
    df = pd.read_csv(path)
    print(path)
    # logger.info("Read File xls File into Dataframe")
    df["address line 1"] = np.nan
    df["address line mid"] = np.nan
    df["city"] = np.nan
    df["state"] = np.nan
    df["zipcode"] = np.nan
    df["applicant info"] = np.nan
    df["contractor"] = np.nan
    df["Owner"] = np.nan
    df["project name"] = np.nan

    file_name = path.split("/")[-1].split(".")[0]
    print(f"file name is {file_name}")

    with open(f"{file_name}-results.csv", "w", newline="") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(
            [
                "Date",
                "Record Number",
                "Record Type",
                "Description",
                "Project Name",
                "Status",
                "Short Notes",
                "address line 1",
                "address line mid",
                "city",
                "state",
                "zipcode",
                "applicant info",
                "contractor",
                "Owner",
                "project name",
            ]
        )
        f_out.close()
    new_rows = []
    for index, row in df.iterrows():

        permit_number_temp = row["Record Number"]
        print("permit_num" + str(permit_number_temp))

        data = scrape_details(
            permit_number=permit_number_temp, file_name=file_name, index=str(index)
        )

        # Change getting
        details = data.get("output")

        row["address line 1"] = details.get("address_line_1")
        row["address line mid"] = details.get("address_line_mid")
        row["city"] = details.get("city")
        row["state"] = details.get("state")
        row["zipcode"] = details.get("zipcode")
        row["applicant info"] = details.get("applicant_info")
        row["contractor"] = details.get("contractor")
        row["owner"] = details.get("owner")
        row["project name"] = details.get("project_name")

        new_rows.append(row)
        time.sleep(random.uniform(0, 3))
        print("number keys in data frame :" + str(len(row.keys())))
        with open(f"{file_name}-results.csv", "a+", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(row.values.flatten().tolist())
        f_out.close()

    modified_df = pd.DataFrame(new_rows)
    name = "modified_" + path.split(r"/")[-1]
    name = name.split(".")[-2] + ".csv"
    print(name)
    modified_df.to_csv(name, index=False)


def scrape_details(permit_number="", file_name="", index=""):
    output = {}
    error = []
    if permit_number is not None:
        try:
            playwright = sync_playwright().start()
            print("playwright started")

            browser = playwright.chromium.launch(headless=True)
            print("webkit launched")

            page = browser.new_page()
            print("new page opened")

            page.goto(BASE_URL)
            print("going to base url")
            page.wait_for_selector(
                "#ctl00_PlaceHolderMain_generalSearchForm_txtGSPermitNumber", state="attached"
            )
            print("search field loaded")

            page.fill("#ctl00_PlaceHolderMain_generalSearchForm_txtGSPermitNumber", permit_number)
            print("data entered")

            page.click("#ctl00_PlaceHolderMain_btnNewSearch > span")
            print("search button clicked")

            page.wait_for_load_state("networkidle")
            print("waited for page to load")

            if page.query_selector(".NotBreakWord"):
                address_data = page.query_selector(".NotBreakWord").inner_text().strip().split("\n")
                address_line_1 = address_data[0].strip()
                address_line_mid = address_data[1].strip() if len(address_data) > 2 else None
                address_line_2_data = (
                    address_data[-1].strip().split(" ") if len(address_data) > 1 else None
                )
                city = address_line_2_data[0] if len(address_line_2_data) > 0 else None
                state = address_line_2_data[1] if len(address_line_2_data) > 1 else None
                zipcode = address_line_2_data[2] if len(address_line_2_data) > 2 else None

                output["address_line_1"] = address_line_1
                output["address_line_mid"] = address_line_mid
                output["address_line_2_data"] = address_line_2_data
                output["city"] = city
                output["state"] = state
                output["zipcode"] = zipcode

            else:
                error.append("address related data is not found")

            if page.query_selector('h1:has(font:text("Applicant Info:"))+ span'):
                applicant_data = (
                    page.query_selector('h1:has(font:text("Applicant Info:"))+ span')
                    .inner_text()
                    .strip()
                )
                applicant_info = ",".join(applicant_data.split("\n")).replace("\t", "")
                output["applicant_info"] = applicant_info

            else:
                error.append("Applicant Data is not found")

            if page.query_selector("#tbl_licensedps > tbody > tr > .td_child_left +td"):
                contractor_data = page.query_selector(
                    "#tbl_licensedps > tbody > tr > .td_child_left +td"
                ).inner_text()
                contractor = ",".join(contractor_data.split("\n")).replace("\t", "")
                output["contractor"] = contractor
            else:
                error.append("contractor data is not found")

            if page.query_selector('h1:has(font:text("Property Owner Info:"))+ span'):
                Owner_data = (
                    page.query_selector('h1:has(font:text("Property Owner Info:"))+ span')
                    .inner_text()
                    .strip()
                )
                Owner = ",".join(Owner_data.split("\n")).replace("\t", "")
                output["Owner"] = Owner

            else:
                error.append("owner data is not found")

            if page.query_selector('h1:has(font:text("Project Name"))+ span'):
                project_name_data = page.query_selector(
                    'h1:has(font:text("Project Name"))+ span'
                ).inner_text()
                project_name = ",".join(project_name_data.split("\n")).replace("\t", "")
                output["project_name"] = project_name
            else:
                error.append("project_name data is not found")

        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            if os.path.exists(f"./errorLinks/{file_name}"):
                with open(f".errorLinks/{file_name}/errors.txt", "a+") as errorfile:
                    errorfile.write("\n")
                    errorfile.write(permit_number)
                errorfile.close()
            else:
                os.makedirs(f"./errorLinks/{file_name}")
                with open(f"./errorLinks/{file_name}/errors.txt", "w") as errorfile:
                    errorfile.write("\n")
                    errorfile.write(permit_number)
                errorfile.close()

        finally:
            json_object = json.dumps(output, indent=4)

            browser.close()
            playwright.stop()

            # Serializing json
            result_dict = {"error": error, "output": output}
            json_object = json.dumps(result_dict, indent=4)

            if os.path.exists(f"./output/{file_name}"):
                with open(f"./output/{file_name}/{file_name}-row-{index}.json", "w") as outfile:
                    outfile.write(json_object)

            else:
                os.makedirs(f"./output/{file_name}/")
                with open(f"./output/{file_name}/{file_name}-row-{index}.json", "w") as outfile:
                    outfile.write(json_object)
            # logger.info(
            #     "scraped data writed to " + f"./output/{file_name}/{file_name}-row-{index}.json"
            # )
            print("resulths final ")
            print(result_dict)
            return result_dict


xlsfiles = [
    "./clear_county/" + f for f in listdir("./clear_county") if isfile(join("./clear_county", f))
]

print(xlsfiles[0])

get_data(path=xlsfiles[0])