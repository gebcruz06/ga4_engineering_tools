from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data import DateRange
from google.analytics.data import Dimension
from google.analytics.data import Metric
from google.analytics.data import Filter
from google.analytics.data import FilterExpression
from google.analytics.data import FilterExpressionList
from google.analytics.data import RunReportRequest
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY
import time

property_id = '#########'
dircred = 'ga4_key.json'

credentials = service_account.Credentials.from_service_account_file(dircred)
client = BetaAnalyticsDataClient(credentials=credentials)

dimensions = [
    Dimension(name="date"),
    Dimension(name="hostName"),
    Dimension(name="pagePathPlusQueryString"),
    Dimension(name="countryId"),
    Dimension(name="deviceCategory"),
    # Dimension(name='customEvent:UserID')
]

metrics = [
    Metric(name="eventCount"),
    Metric(name="sessions"),
    Metric(name="engagedSessions"),
    Metric(name="screenPageViews"),
    Metric(name="userEngagementDuration"),
]

# Enable to fetch last 7 days of data (here: last 3 to 1 days ago)
dateLbound = datetime.now() - timedelta(3)
dateUbound = datetime.now() - timedelta(1)

ctr = 0

# Define all page paths you want to match
page_paths = [
    "/proof-test-estimator/",
    "/Pages/calculators-flow-calculator.aspx",
    "/Pages/calculators-conversion.aspx",
    "/Pages/product-configurators.aspx",
    "/Pages/pneumatic-valve-configurators.aspx",
    "/numacalc/homePage",
    "/client_area/index.php",
    "/ams/wirelessplanningtool/",
    "/rohssearch/index.aspx",
    "/advisor/",
    "/phasecalculator/",
    "/oildensityref/",
    "/automation/measurement-instrumentation/discontinued-measurement-instrumentation",
    "/automation/electrical-component-lighting/lighting/calculator",
    "/automation/measurement-instrumentation/liquid-analysis/wiring-diagram",
    "/catalog/RMTProductAdvisorToolsDisplayView",
    "/catalog/MMProductAdvisorToolsDisplayView",
    "/catalog/DanielProductAdvisorToolsDisplayView",
    "/catalog/GlobalConfigurationSystemToolDisplayView",
    "/catalog/MMProLinkToolsDisplayView",
    "/catalog/ARRAComplianceSearchView",
    "/catalog/MM5700UploadToolsDisplayView",
    "/catalog/DanielSparePart3DToolDisplayView",
    "/automation/digital/engineering-tools",
    "/ioondemandcalculator/",
    "/rosemount/liquidanalysiswiringdiagram/default.aspx",
    "/rosemount/powermodulelifecalculator/default.aspx",
    "/rosemount/swirelessgcestimator/default.html",
    "/rosemount/dp_flow/application/pages/pcdefault.aspx",
    "/rosemount-toolkitinstall-en",
    "/rosemount/dp_level/application/pages/pcdefault.aspx",
    "/rosemount/tankgauging/calculator/index.html",
    "/rosemount/modelcodeconverttool/mct.aspx",
    "/rosemount/thumwiringwebapps/default.aspx",
    "/rosemount/wirelessestimator/default.aspx",
    "thermowelldesign.emerson.com",
    "/sizing"
]

# Build OR filter expressions programmatically
filters = [
    FilterExpression(
        filter=Filter(
            field_name="pagePath",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value=path
            )
        )
    )
    for path in page_paths
]

# Daily appending queries to work around GA sampling
for datebound in rrule(DAILY, dtstart=dateLbound, until=dateUbound):
    datebound = datetime.strftime(datebound, '%Y-%m-%d')

    offset = 0
    endloop = False

    while not endloop:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=datebound, end_date=datebound)],
            dimension_filter=FilterExpression(
                or_group=FilterExpressionList(expressions=filters)
            ),
            limit=100000,
            offset=offset
        )

        response = client.run_report(request)

        # Handle pagination
        if response.row_count > 100000:
            offset += 100000
        else:
            offset = response.row_count

        if offset >= response.row_count:
            endloop = True

        # Show progress
        if response.row_count > 100000 and offset < response.row_count:
            print(f'{datebound}: {offset} of {response.row_count} rows received')
        else:
            print(f'{datebound}: {response.row_count} of {response.row_count} rows received')

        # Extract dimension & metric names
        dims = [d.name for d in response.dimension_headers]
        mets = [m.name for m in response.metric_headers]

        # Extract rows into lists
        dimlist = [[dv.value for dv in row.dimension_values] for row in response.rows]
        metlist = [[mv.value for mv in row.metric_values] for row in response.rows]

        # Create DataFrames
        dimdf = pd.DataFrame(dimlist, columns=dims)
        metdf = pd.DataFrame(metlist, columns=mets)

        # Merge into one DataFrame
        odf = pd.concat([dimdf, metdf], axis=1)

        # Write to CSV (append mode after first run)
        if ctr == 0:
            odf.to_csv('GA4 API Engineering Tools.csv', index=False, encoding="utf-8-sig")
        else:
            odf.to_csv('GA4 API Engineering Tools.csv', mode='a', index=False, header=False, encoding="utf-8-sig")

        ctr += 1
        time.sleep(1.2)

# Code written by: Geber Cruz <geber.b.cruz@gmail.com>
