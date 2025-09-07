import pandas as pd
import matplotlib.pyplot as plt
import calmap
import matplotlib as mpl
from matplotlib.colors import LinearSegmentedColormap

cmap_name = "violetEA"
if cmap_name not in mpl.colormaps:
    custom_cmap = LinearSegmentedColormap.from_list(
        cmap_name,
        ["#E6E7FF", "#6567EA", "#1E1F66"],  # clair → médian → foncé
        N=256
    )
    mpl.colormaps.register(custom_cmap, name=cmap_name)


def plot_calendar(
    df,
    year_min=None,
    year_max=None,
    max_dist=None,
    fig_height=15,
    fig_width=9,
):
    ACTIVITY_FORMAT = "%b %d, %Y, %H:%M:%S %p"

    # Process data
    df["start_date"] = pd.to_datetime(df["start_date"], format=ACTIVITY_FORMAT)
    df["date"] = df["start_date"].dt.date
    df = df.groupby("date")["distance"].sum()
    df.index = pd.to_datetime(df.index)
    df.clip(0, max_dist, inplace=True)

    if year_min:
        df = df[df.index.year >= year_min]
    if year_max:
        df = df[df.index.year <= year_max]

    # Récupération de la colormap via la nouvelle API

    # Create heatmap
    fig, ax = calmap.calendarplot(
        data=df,
        cmap=mpl.colormaps.get_cmap("violetEA"),
        fillcolor="white"    # couleur des jours sans activité
        )

    # Ajuster taille
    fig.set_figheight(fig_height)
    fig.set_figwidth(fig_width)

    return fig
