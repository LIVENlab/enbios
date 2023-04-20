# import tempfile
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from pandas.core.groupby import DataFrameGroupBy

from sklearn.cluster import KMeans
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from matplotlib.colors import ListedColormap


def clean_data(df: DataFrame) -> DataFrame:
    # kick out scope = Internal
    df = df[df.Scope != "Internal"]
    # kick out scope, period, unit cuz it is all the same
    df.drop(columns=["Scope", "Period", "Unit"], inplace=True)
    # kick out all where Processor is "environment"
    df = df[df.Processor != "environment"]
    # transform the Scenario column to int
    df["Scenario"] = df['Scenario'].str.replace('_', '').astype(int)
    # Optional, calculate the level, and System group.
    # add a new column of type int, by splitting the processor by "." and counting the length of the array
    df["Level"] = df["Processor"].str.split(".").apply(len).astype(int)

    return df


def add_systems_to_total_rows(df: DataFrame,
                              total_system_name: str,
                              group: list[str] = ("Scenario", "Processor", "Indicator"),
                              ) -> DataFrame:
    group = list(group)  # cuz pandas doesnt like tuples
    df["Systemgroup"] = "region"
    # add a row for the total of the scenario
    grouped_df = df.groupby(group).agg({"Value": "sum"}).reset_index()
    grouped_df["System"] = total_system_name
    grouped_df["Systemgroup"] = "total"
    grouped_df["Level"] = grouped_df["Processor"].str.split(".").apply(len).astype(int)

    # Reorder the columns to match the original DataFrame
    grouped_df = grouped_df[["Scenario", "System", "Systemgroup", "Processor", "Level", "Indicator", "Value"]]
    # Concatenate the original DataFrame with the aggregated data
    result_df = pd.concat([df, grouped_df]).reset_index(drop=True)
    result_df = result_df.sort_values(group)
    return result_df


def add_normalized_col(df: DataFrame, groups: list[str] = ("Level", "Indicator")) -> DataFrame:
    def apply_min_max_scaler(group: DataFrameGroupBy) -> DataFrameGroupBy:
        scaler = MinMaxScaler()
        scaled_values = scaler.fit_transform(group['Value'].to_numpy().reshape(-1, 1))
        group['norm_value'] = scaled_values
        return group

    return df.groupby(list(groups)).apply(apply_min_max_scaler).reset_index(drop=True)


def cluster_on_processor(df: DataFrame, processor: str, n_clusters=3) -> DataFrame:
    if "norm_value" not in df.columns:
        raise ValueError("The DataFrame must contain a column named 'norm_value', run 'add_normalized_col' first.")
    # Filter the DataFrame based on the indicator and processor
    df_filtered = df[df['Processor'] == processor]
    df_pivot = df_filtered.pivot_table(index='Scenario', columns='Indicator', values='norm_value')

    # Run KMeans clustering
    X = df_pivot.values
    kmeans = KMeans(n_clusters=n_clusters, n_init=10)
    cluster_labels = kmeans.fit_predict(X)

    # Create a new DataFrame with the required format
    df_result = df_pivot.reset_index()
    df_result.columns = [f"Value_{col}" if col != 'Scenario' else col for col in df_result.columns]
    df_result['Label'] = cluster_labels
    df_result['Processor'] = processor

    # Merge the original values back into the DataFrame
    df_result = df_result.melt(id_vars=['Scenario', 'Label', 'Processor'], var_name='Indicator',
                               value_name='scaled_value')
    df_result['Indicator'] = df_result['Indicator'].str.replace('Value_', '')
    df_result = df_result.merge(df_filtered[['Scenario', 'Indicator', 'Value']], on=['Scenario', 'Indicator'],
                                how='left')

    return df_result


def plot_heatmaps_stacked(df_clustered: DataFrame,
                          plot_image_path: str,
                          sort: bool = True):
    if not (parent_path := Path(plot_image_path).parent).exists():
        raise ValueError(f"The directory {parent_path.as_posix()} does not exist.")
    indicators = df_clustered['Indicator'].unique()
    n_indicators = len(indicators)
    scenarios = df_clustered['Scenario'].unique()
    n_scenarios = len(scenarios)

    # Create a colormap based on the cluster labels
    unique_labels = df_clustered['Label'].unique()
    colormap = ListedColormap(sns.color_palette("husl", len(unique_labels)))

    # Set the style and size of the plots
    sns.set(style='white')
    fig, axes = plt.subplots(n_indicators, 1, figsize=(n_scenarios / 6, n_indicators * 3), sharex=True)

    # Create a heatmap for each indicator
    for idx, indicator in enumerate(indicators):
        # Sort the DataFrame by cluster label for the current indicator
        df_indicator = df_clustered[df_clustered['Indicator'] == indicator].sort_values(by='Label')

        # Sort the scenarios based on the label/group
        if sort:
            sorted_scenarios = df_indicator.drop_duplicates(subset='Scenario', keep='first').sort_values(by='Label')[
                'Scenario'].tolist()
        else:
            sorted_scenarios = df_indicator.drop_duplicates(subset='Scenario', keep='first')['Scenario'].tolist()

        data = df_indicator.pivot_table(index='Indicator', columns='Scenario', values='Value')[sorted_scenarios]

        # Create a custom color mapping for the x-axis labels based on the cluster labels
        cluster_colors = [colormap(
            df_clustered.loc[(df_clustered['Scenario'] == s) & (df_clustered['Indicator'] == indicator), 'Label'].iloc[
                0]) for s in sorted_scenarios]

        sns.heatmap(data, cmap='viridis', ax=axes[idx], cbar_kws={'label': 'Value'})

        # Set plot title and labels
        axes[idx].set_title(f'Heatmap of {indicator} by Scenario')
        axes[idx].set_xticks(range(len(sorted_scenarios)))
        axes[idx].set_xticklabels(sorted_scenarios, fontsize=8, ha="left")

        # Set custom colors for each xticklabel
        for tick, color in zip(axes[idx].get_xticklabels(), cluster_colors):
            tick.set_color(color)
            # tick.set_ha("center")
        if idx == 0:
            axes[idx].set_xlabel('Scenario')

        axes[idx].set_ylabel('')

    # Show the plot
    plt.tight_layout()
    # plt.show()
    fig.savefig(plot_image_path)
    return plt
