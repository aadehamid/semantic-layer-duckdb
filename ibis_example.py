import marimo

__generated_with = "0.16.2"
app = marimo.App(width="medium", auto_download=["ipynb"])


@app.cell
def _():
    import marimo as mo
    import ibis
    return (ibis,)


@app.cell
def _(ibis):
    con = ibis.connect("duckdb://penguins.ddb")
    con.create_table(
        "penguins", ibis.examples.penguins.fetch().to_pyarrow(), overwrite=True
    )
    return (con,)


@app.cell
def _(con):
    con.list_tables()
    return


@app.cell
def _(con):
    penguins = con.table("penguins")
    penguins
    return (penguins,)


@app.cell
def _(penguins):
    penguins.head().to_pandas()
    return


@app.cell
def _(ibis, penguins):
    ibis.options.interactive = True
    penguins.head()
    return


@app.cell
def _(penguins):
    penguins.filter(penguins.species == "Adelie")
    return


@app.cell
def _(penguins):
    penguins.filter(
        (penguins.species == "Adelie") & (penguins.island == "Torgersen")
    )
    return


@app.cell
def _(penguins):
    penguins.select("species", "island", "year")
    return


@app.cell
def _(ibis, penguins):
    penguins.mutate(
        bill_length_cm=penguins.bill_length_mm / 10,
        continent=ibis.literal("Antarctica"),
    ).select(ibis.selectors.matches(r"^(continent|bill_length_cm)$"))

    # select("continent", "bill_length_cm")
    return


@app.cell
def _(penguins):
    penguins.order_by(penguins.flipper_length_mm).select(
        "species", "island", "flipper_length_mm"
    )
    return


@app.cell
def _(ibis, penguins):
    penguins.order_by(ibis.desc("flipper_length_mm")).select(
        "species", "island", "flipper_length_mm"
    )
    return


@app.cell
def _(penguins):
    penguins.group_by(["species", "island"]).aggregate()
    return


@app.cell
def _(penguins):
    penguins.group_by(["species", "island"]).aggregate(
        [penguins.bill_length_mm.mean(), penguins.flipper_length_mm.max()]
    )
    return


@app.cell
def _(penguins):
    penguins.group_by(["species", "island", "sex"]).aggregate(
        [penguins.bill_length_mm.mean(), penguins.flipper_length_mm.max()]
    )
    return


@app.cell
def _(penguins):
    penguins.filter((penguins.sex == "female"), (penguins.year == 2008)).group_by(
        ["island"]
    ).aggregate(penguins.body_mass_g.max())
    return


@app.cell
def _(penguins):
    penguins.filter(penguins.sex == "male").group_by(["island", "year"]).aggregate(
        penguins.body_mass_g.max().name("max_body_mass")
    ).order_by(["year", "max_body_mass"])
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
