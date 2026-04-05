from __future__ import annotations

import argparse
import html
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol
import boto3

import pandas as pd
from dotenv import load_dotenv

from src.jobs.notion_transform import transform_all_notion_data
from src.jobs.whoop_transform import transform_all_whoop_data
from src.shared.logger import get_logger

# Create global logging object
logger = get_logger(__name__)

DEFAULT_LOOKBACK_DAYS = '7'
DEFAULT_OUTPUT_PATH = 'tmp/weekly_report.html'


@dataclass(frozen=True)
class ReportSection:
    '''
    DataClass for email section containing Section Title
    and corresponding DataFrame.
    '''
    title: str
    dataframe: pd.DataFrame


@dataclass(frozen=True)
class WeeklyReportPayload:
    '''
    Full report payload for weekly report email.
    '''
    subject: str
    report_start: pd.Timestamp
    report_end: pd.Timestamp
    sections: tuple[ReportSection, ...]


class EmailSender(Protocol):
    def send(self, payload: WeeklyReportPayload, html_body: str) -> str:
        '''
        Deliver the rendered report and return a delivery summary.
        '''


class DryRunEmailSender:
    '''
    Email sender for dry-runs.
    '''
    
    def __init__(self, output_path: str) -> None:
        self.output_path = Path(output_path)

    def send(self, payload: WeeklyReportPayload, html_body: str) -> str:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text(html_body, encoding='utf-8')
        return f'dry-run output written to {self.output_path}'


class SesEmailSender:
    '''
    AWS SES email sender class.
    '''
    
    def __init__(self) -> None:
        # Store AWS SES sender reqs from environment variables
        self.sender = os.environ['WEEKLY_REPORT_SENDER']
        self.recipient = os.environ['WEEKLY_REPORT_RECIPIENT']
        self.region = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION') or 'us-east-1'

    def send(self, payload: WeeklyReportPayload, html_body: str) -> str:
        '''
        Send function that utilizes AWS SES to send weekly report.

        Args:
            payload (WeeklyReportPayload): full weekly report payload
            html_body (str): email HTML email body section

        Returns:
            str: recipient and message id info
        '''
        # Create SES client
        client = boto3.client('ses', region_name=self.region)
        
        # Call send email function, store response
        response = client.send_email(
            Source=self.sender,
            Destination={'ToAddresses': [self.recipient]},
            Message={
                'Subject': {'Data': payload.subject, 'Charset': 'UTF-8'},
                'Body': {
                    'Html': {'Data': html_body, 'Charset': 'UTF-8'},
                },
            },
        )
        
        # Retrieve message for return purposes
        message_id = response.get('MessageId', 'unknown')
        
        return f'sent via SES to {self.recipient} message_id={message_id}'


def parse_args() -> argparse.Namespace:
    '''
    Argument parser allowing for script to run in dry mode and send mode.
    
    --send
    This is a boolean flag. If present, the job sends the report via SES. If omitted, the job stays in dry-run mode.
    
    --output-path
    This sets where the dry-run HTML file should be written. If the flag is not provided, it falls back to WEEKLY_REPORT_OUTPUT_PATH from
    the environment, and then to the hardcoded default tmp/weekly_report.html.
    '''
    # Initialize argument parser object
    parser = argparse.ArgumentParser(description='Generate and optionally send the weekly health report email.')
    
    # Add send argument
    parser.add_argument(
        '--send',
        action='store_true',
        help='Send the report via AWS SES. Default behavior is dry-run output to a file.',
    )
    
    # Add output path argument
    parser.add_argument(
        '--output-path',
        default=os.environ.get('WEEKLY_REPORT_OUTPUT_PATH', DEFAULT_OUTPUT_PATH),
        help='Dry-run HTML output path.',
    )
    return parser.parse_args()


def resolve_report_window() -> tuple[pd.Timestamp, pd.Timestamp]:
    '''
    Resolve the last completed 7-day reporting window in UTC.
    '''
    end_date = pd.Timestamp.now(tz='UTC').normalize() - pd.Timedelta(days=1)
    start_date = end_date - pd.Timedelta(days=6)
    return start_date, end_date


def build_date_spine(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    '''
    Build a daily date spine across the requested report window.
    '''
    return pd.DataFrame({'date': pd.date_range(start=start_date, end=end_date, freq='D', tz='UTC')})


def coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    '''
    Coerce selected columns to numeric when present.
    '''
    df = df.copy()
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
    return df


def filter_to_report_window(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    '''
    Filter a daily DataFrame to the report window.
    '''
    # Handle empty DataFrame
    if df.empty:
        return df.copy()

    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    return df.loc[mask].sort_values('date').reset_index(drop=True)


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    '''
    Return the requested columns, backfilling missing ones with nulls.
    '''
    df = df.copy()
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df.loc[:, columns]


def build_workout_duration_daily(workouts_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Aggregate workout duration by date using transformed workout timestamps.
    '''
    
    columns = ['date', 'total_workout_duration_hours']
    
    # Handle empty DataFrame
    if workouts_df.empty:
        return pd.DataFrame(columns=columns)

    # Convert timestamps and calculate the workout duration, store in column
    duration_df = workouts_df.copy()
    duration_df['workout_duration_hours'] = (
        pd.to_datetime(duration_df['workout_end'], utc=True, errors='coerce')
        - pd.to_datetime(duration_df['workout_start'], utc=True, errors='coerce')
    ).dt.total_seconds() / 3_600
    duration_df['workout_duration_hours'] = duration_df['workout_duration_hours'].clip(lower=0)

    # Aggregate all workout durations, per day
    summary_df = (
        duration_df.groupby('date', dropna=True)
        .agg(total_workout_duration_hours=('workout_duration_hours', 'sum'))
        .reset_index()
    )
    return ensure_columns(summary_df, columns)


def fetch_report_sources() -> tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Load transformed daily WHOOP and Notion datasets and enrich WHOOP daily rows
    with workout duration totals.
    '''
    # Retrieve lookback time frame, revert back to default
    os.environ.setdefault('REPORT_LOOKBACK_DAYS', DEFAULT_LOOKBACK_DAYS)

    # Transform WHOOP and Notion data
    whoop_outputs = transform_all_whoop_data()
    notion_outputs = transform_all_notion_data()

    # Store WHOOP daily DataFrame, capture workout duration
    whoop_daily_df = whoop_outputs['daily'].copy()
    workout_duration_daily_df = build_workout_duration_daily(whoop_outputs['workouts'])
    if 'total_workout_duration_hours' not in whoop_daily_df.columns:
        whoop_daily_df = whoop_daily_df.merge(workout_duration_daily_df, on='date', how='left')

    # Store Notion daily DataFrame
    notion_daily_df = notion_outputs['daily'].copy()

    logger.info(
        'Loaded transformed report sources whoop_daily_rows=%s notion_daily_rows=%s',
        len(whoop_daily_df),
        len(notion_daily_df),
    )
    return whoop_daily_df, notion_daily_df


def format_date_label(date_value: pd.Timestamp) -> str:
    '''
    Formats date time stamp to String value.

    Args:
        date_value (pd.Timestamp): date timestamp

    Returns:
        str: timestamp as String
    '''
    return pd.Timestamp(date_value).strftime('%a %Y-%m-%d')


def format_number(value: object, decimals: int = 1) -> str:
    '''
    Formats number value to a Stringed float, specifying number of decimals.

    Args:
        value (object): number value
        decimals (int, optional): number of decimals. Defaults to 1.

    Returns:
        str: String float value
    '''
    # Handle invalid value
    if value is None or pd.isna(value):
        return 'N/A'
    
    return f'{float(value):.{decimals}f}'


def format_integer(value: object) -> str:
    '''
    Formats number value to a Stringed integer.

    Args:
        value (object): number value

    Returns:
        str: Stringed integer value
    '''
    # Handle invalid value
    if value is None or pd.isna(value):
        return 'N/A'
    
    return str(int(round(float(value))))


def format_percent(value: object) -> str:
    '''
    Formats number value to a percentage.

    Args:
        value (object): percentage value

    Returns:
        str: Stringed percentage value
    '''
    # Handle missing value
    if value is None or pd.isna(value):
        return 'N/A'
    
    return f'{float(value):.0f}%'


def finalize_display_dataframe(df: pd.DataFrame, formatters: dict[str, Callable[[object], str]]) -> pd.DataFrame:
    '''
    Prepare DataFrame for email rendering. Applies String formatting to 
    date column and renames column for output purposes.

    Args:
        df (pd.DataFrame): DataFrame being prepared
        formatters (dict[str, Callable[[object], str]]): rename dict

    Returns:
        pd.DataFrame: email ready DataFrame
    '''
    # Apply String formatting to date column for output purposes
    display_df = df.copy()
    display_df['date'] = display_df['date'].apply(format_date_label)

    # Rename columns based on passed in dict
    for column, formatter in formatters.items():
        if column in display_df.columns:
            display_df[column] = display_df[column].apply(formatter)

    return display_df


def build_strain_workouts_section(whoop_daily_df: pd.DataFrame, date_spine: pd.DataFrame) -> ReportSection:
    '''
    WHOOP Strain email section driver function. Takes WHOOP daily DataFrame
    and trims down to the date window. Executes validation checks and gets
    DataFrame email-ready.

    Args:
        whoop_daily_df (pd.DataFrame): daily WHOOP DataFrame
        date_spine (pd.DataFrame): date window DataFrame

    Returns:
        ReportSection: WHOOP Strain email section
    '''
    # Define the requested columns
    columns = [
        'date',
        'cycle_strain',
        'workout_count',
        'total_workout_duration_hours',
        'total_strain',
    ]

    # Create base DataFrame, merge based on date window and ensure expected columns
    base_df = date_spine.merge(ensure_columns(whoop_daily_df, columns), on='date', how='left')
    
    # Format numeric columns, fill missing values with 0
    base_df = coerce_numeric(base_df, columns[1:])
    base_df[['workout_count', 'total_workout_duration_hours', 'total_strain']] = base_df[
        ['workout_count', 'total_workout_duration_hours', 'total_strain']
    ].fillna(0)

    # Create email-ready DataFrame
    display_df = finalize_display_dataframe(
        base_df.rename(
            columns={
                'cycle_strain': 'Daily Strain',
                'workout_count': 'Workout Count',
                'total_workout_duration_hours': 'Workout Hours',
                'total_strain': 'Workout Strain',
            }
        ),
        formatters={
            'Daily Strain': lambda value: format_number(value, 1),
            'Workout Count': format_integer,
            'Workout Hours': lambda value: format_number(value, 2),
            'Workout Strain': lambda value: format_number(value, 1),
        },
    )
    display_df = display_df.rename(columns={'date': 'Day'})
    return ReportSection(title='Strain / Workouts (WHOOP)', dataframe=display_df)


def build_recovery_section(whoop_daily_df: pd.DataFrame, date_spine: pd.DataFrame) -> ReportSection:
    '''
    WHOOP Recovery email section driver function. Takes WHOOP daily DataFrame
    and trims down to the date window. Executes validation checks and gets
    DataFrame email-ready.

    Args:
        whoop_daily_df (pd.DataFrame): daily WHOOP DataFrame
        date_spine (pd.DataFrame): date window DataFrame

    Returns:
        ReportSection: WHOOP Recovery email section
    '''
    # Define the requested columns
    columns = [
        'date',
        'recovery_score',
        'resting_heart_rate',
        'heart_rate_variability_rmssd',
        'total_sleep_duration_hours',
    ]

    # Create base DataFrame, merge based on date window and ensure expected columns
    base_df = date_spine.merge(ensure_columns(whoop_daily_df, columns), on='date', how='left')
    
    # Convert metric columns to numeric types for formatting
    base_df = coerce_numeric(base_df, columns[1:])

    # Prepare the DataFrame and apply display formatting
    display_df = finalize_display_dataframe(
        base_df.rename(
            columns={
                'recovery_score': 'Recovery Score',
                'resting_heart_rate': 'Resting HR',
                'heart_rate_variability_rmssd': 'HRV RMSSD',
                'total_sleep_duration_hours': 'Sleep Hours',
            }
        ),
        formatters={
            'Recovery Score': format_percent,
            'Resting HR': format_integer,
            'HRV RMSSD': lambda value: format_number(value, 1),
            'Sleep Hours': lambda value: format_number(value, 2),
        },
    )
    
    # Rename date column to email-output day column
    display_df = display_df.rename(columns={'date': 'Day'})
    return ReportSection(title='Recovery (WHOOP)', dataframe=display_df)


def build_calories_weight_protein_section(notion_daily_df: pd.DataFrame, date_spine: pd.DataFrame) -> ReportSection:
    '''
    Notion nutrition email section driver function. Takes Notion daily DataFrame
    and trims down to the date window. Executes validation checks and gets
    DataFrame email-ready.

    Args:
        notion_daily_df (pd.DataFrame): daily Notion DataFrame
        date_spine (pd.DataFrame): date window DataFrame

    Returns:
        ReportSection: Notion nutrition email section
    '''
    # Define the requested columns
    columns = [
        'date',
        'calories',
        'weight_lbs',
        'protein_g',
    ]

    # Create base DataFrame, merge based on date window and ensure expected columns
    base_df = date_spine.merge(ensure_columns(notion_daily_df, columns), on='date', how='left')
    
    # Convert metric columns to numeric types for filling and formatting
    base_df = coerce_numeric(base_df, columns[1:])
    
    # Fill additive nutrition metrics and carry weight values across the week
    base_df[['calories', 'protein_g']] = base_df[['calories', 'protein_g']].fillna(0)
    base_df['weight_lbs'] = base_df['weight_lbs'].ffill().bfill()

    # Prepare the DataFrame and apply display formatting
    display_df = finalize_display_dataframe(
        base_df.rename(
            columns={
                'calories': 'Calories',
                'weight_lbs': 'Weight (lbs)',
                'protein_g': 'Protein (g)',
            }
        ),
        formatters={
            'Calories': format_integer,
            'Weight (lbs)': lambda value: format_number(value, 1),
            'Protein (g)': format_integer,
        },
    )
    
    # Rename date column to email-output day column
    display_df = display_df.rename(columns={'date': 'Day'})
    return ReportSection(title='Calories / Weight / Protein', dataframe=display_df)


def build_weekly_report_payload() -> WeeklyReportPayload:
    '''
    Weekly report driver function. Resolves the report window, filters all
    source DataFrames down to the target week, and builds the final email
    payload with all report sections.

    Returns:
        WeeklyReportPayload: subject, date window, and section payloads
    '''
    # Resolve weekly date window and build matching date spine
    start_date, end_date = resolve_report_window()
    date_spine = build_date_spine(start_date, end_date)
    
    # Fetch transformed daily source DataFrames
    whoop_daily_df, notion_daily_df = fetch_report_sources()
    
    # Filter source DataFrames down to the report week
    whoop_week_df = filter_to_report_window(whoop_daily_df, start_date, end_date)
    notion_week_df = filter_to_report_window(notion_daily_df, start_date, end_date)

    # Build ordered email sections from filtered source DataFrames
    sections = (
        build_strain_workouts_section(whoop_week_df, date_spine),
        build_recovery_section(whoop_week_df, date_spine),
        build_calories_weight_protein_section(notion_week_df, date_spine),
    )
    
    # Construct email subject line with date range
    subject = f'Weekly Health Report: {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}'
    return WeeklyReportPayload(
        subject=subject,
        report_start=start_date,
        report_end=end_date,
        sections=sections,
    )


def render_html_table(df: pd.DataFrame) -> str:
    '''
    Create HTML table based on given DataFrame.

    Args:
        df (pd.DataFrame): email-ready DataFrame

    Returns:
        str: HTML table
    '''
    # Create table headers
    header_cells = ''.join(f'<th>{html.escape(column)}</th>' for column in df.columns)
    
    # Populate table with rows
    row_html: list[str] = []
    for _, row in df.iterrows():
        cells = ''.join(f'<td>{html.escape(str(value))}</td>' for value in row.tolist())
        row_html.append(f'<tr>{cells}</tr>')
    return (
        '<table>'
        f'<thead><tr>{header_cells}</tr></thead>'
        f'<tbody>{"".join(row_html)}</tbody>'
        '</table>'
    )


def render_email_html(payload: WeeklyReportPayload) -> str:
    '''
    Create HTML email rendering with WeeklyReportPayload.

    Args:
        payload (WeeklyReportPayload): weekly report data

    Returns:
        str: HTML email rendering
    '''    
    # Create HTML section storage
    sections_html: list[str] = []
    
    # Append all sections to list
    for section in payload.sections:
        sections_html.append(
            '<section class="report-section">'
            f'<h2>{html.escape(section.title)}</h2>'
            f'{render_html_table(section.dataframe)}'
            '</section>'
        )

    return f'''
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>{html.escape(payload.subject)}</title>
    <style>
      body {{
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: #f5f7fb;
        color: #14213d;
        margin: 0;
        padding: 24px;
      }}
      .container {{
        max-width: 960px;
        margin: 0 auto;
        background: #ffffff;
        border-radius: 16px;
        padding: 32px;
        box-shadow: 0 10px 30px rgba(20, 33, 61, 0.08);
      }}
      h1 {{
        margin-top: 0;
        margin-bottom: 8px;
      }}
      p.subtitle {{
        margin-top: 0;
        color: #4a5568;
      }}
      .report-section {{
        margin-top: 28px;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        margin-top: 12px;
        font-size: 14px;
      }}
      th, td {{
        padding: 10px 12px;
        border-bottom: 1px solid #dbe2ea;
        text-align: left;
      }}
      th {{
        background: #edf2f7;
      }}
      tbody tr:nth-child(even) {{
        background: #f9fbfd;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      <h1>Weekly Health Report</h1>
      <p class="subtitle">{html.escape(payload.report_start.strftime("%Y-%m-%d"))} to {html.escape(payload.report_end.strftime("%Y-%m-%d"))}</p>
      {''.join(sections_html)}
    </div>
  </body>
</html>
'''.strip()


def build_email_sender(args: argparse.Namespace) -> EmailSender:
    '''
    Handles argument for dry-run or email sender.

    Args:
        args (argparse.Namespace): argument passed in

    Returns:
        EmailSender: Email Sender for dry runs 
    '''
    if args.send:
        return SesEmailSender()
    return DryRunEmailSender(args.output_path)


def main() -> int:
    '''
    Driver function for the weekly report job.

    Returns:
        int: Exit code.
    '''
    # Load environment variables
    load_dotenv()
    
    # Parse arguments
    args = parse_args()

    logger.info('Starting weekly report job')

    try:
        # Build weekly report payload
        payload = build_weekly_report_payload()
        
        # Render HTML email body
        html_body = render_email_html(payload)
        
        # Build email sender
        sender = build_email_sender(args)
        
        # Send weekly report via email
        delivery_summary = sender.send(payload, html_body)
        logger.info('Weekly report completed delivery=%s', delivery_summary)
        return 0
    except Exception:
        logger.exception('Weekly report job failed')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
