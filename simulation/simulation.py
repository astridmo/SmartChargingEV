from gurobipy import *
import pandas as pd
from datetime import timedelta

def run_simulation(start_time_utc, peak_tariff, df_overview_csv, df_spotprice_csv, power_method):
    """Function to run the simulation"""

    df_spotprice = get_relevant_spotprices(df_spotprice_csv, start_time_utc)

    df_overview = get_df_overview(df_overview_csv)

    # Run optimization
    optimization_results = optimize_charging(df_overview, df_spotprice, peak_tariff, start_time_utc, power_method)



    return optimization_results


def get_relevant_spotprices(df_spotprice_csv, start_time_utc):
    """
    Function to return the spotprice-dataframe starting with the chosen start time
    :param df_spotprice_csv: DataFram of spotprices
    :param start_time_utc: The chosen start time (in UTC)
    :return: DataFrame of spotprices, starting whith the chosen start time
    """
    df_spotprice = pd.read_csv(df_spotprice_csv, index_col="DateTimeUtc")  # Load csv
    df_spotprice.index = pd.to_datetime(df_spotprice.index) # Change index to datatime dtype

    # Make the DataFrame start with the chosen start time
    if start_time_utc in df_spotprice.index:
        df_spotprice = df_spotprice[~(df_spotprice.index < start_time_utc)]  # Delete all rows before start_time_utc
    else:
        raise ValueError('There is no spot price data available for the chosen dates')
    return df_spotprice


def get_df_overview(df_overview_csv):
    # Load files
    df_overview = pd.read_csv(df_overview_csv)

    # Change DateTime-columns to pandas DateTime
    df_overview["StartDateTime"] = pd.to_datetime(df_overview["StartDateTime"])
    df_overview["EndDateTime"] = pd.to_datetime(df_overview["EndDateTime"])

    # To get fewer cars
    #df_overview_simulation = df_overview[:200]
    df_overview_simulation = df_overview.copy()  # All cars

    if len(df_overview_simulation) != len(df_overview):  # Hvis man ikke studerer alle bilene
        df_overview_simulation = _new_starthour(df_overview_simulation)

    return df_overview_simulation


def _new_starthour(df_overview_simulation):
    """
    Denne funksjonen gjør kun en endring på original dataframe dersom ikke alle bilene blir studert.

    Funksjonen gjør at man starter på StartHour = 0.
    :param df_overview_simulation: DataFrame
    :return: df_overview der den første bilen har StartHour = 0.
    """
    # Number of hours between the departure of the last car and the arrival of car 1
    start_time = df_overview_simulation["StartDateTime"].min()  # The oldest datetime (start time for the first car)

    # Lag ankomsttimer til int, der den første timen er int 0. Timen etter dette er int 1.
    df_overview_simulation['StartHour'] = (df_overview_simulation.loc[:,'StartDateTime'] - start_time) // pd.Timedelta('1H')
    # Gjør også dette med avreisetidspunkt ved å ta utgangspunkt i int 0 fra StartHour
    df_overview_simulation['EndHour'] = (df_overview_simulation.loc[:,'EndDateTime'] - start_time) // pd.Timedelta('1H')

    df_overview_simulation = df_overview_simulation.reset_index(drop=True)

    print("Den siste timen er: ", df_overview_simulation["EndDateTime"].max())
    return df_overview_simulation


def optimize_charging(df_overview, df_spotprice, peak_costs, start_time_utc, power_method):
    # Antall elbiler
    N_cars = len(df_overview)

    # Number of hours between the departure of the last car and the arrival of car 1
    n_hours = int(df_overview["EndHour"].max())

    # Generer en tidsperiode fra start til slutt per time
    start_time_utc = pd.to_datetime(start_time_utc)
    print(n_hours)
    print(type(n_hours))
    print(timedelta(hours=n_hours))
    print(f"start_time {start_time_utc} |, timedelta: {timedelta(hours=n_hours)}, dtype:")
    all_dates = pd.date_range(start=start_time_utc, end=start_time_utc + timedelta(hours=n_hours), freq='H')

    # Lag en mapping fra hver time til tilsvarende måned
    hours_to_month = {hour: date.month for hour, date in enumerate(all_dates)}

    unique_months = set(hours_to_month.values())  # Hent de unike månedene


    class ElectricVehicle:
        def __init__(self, arrival_time, departure_time, required_charge, max_charge_rate):
            self.arrival_time = arrival_time
            self.departure_time = departure_time
            self.required_charge = required_charge
            self.max_charge_rate = max_charge_rate


    # Henter inn detaljer om bilene
    arrival_time = df_overview['StartHour']
    departure_time = df_overview['EndHour']
    required_charge = df_overview['Energy']
    if power_method == "Reell":
      max_charge_rate_per_vehicle = df_overview['Max_power [kW]']
      # Make the vehicles
      vehicles = [ElectricVehicle(arrival_time[i], departure_time[i], required_charge[i], max_charge_rate_per_vehicle[i]) for i in range(N_cars)]

    elif power_method == "22":
      max_charge_rate_per_vehicle = 22.1
      # Make the vehicles
      vehicles = [ElectricVehicle(arrival_time[i], departure_time[i], required_charge[i], max_charge_rate_per_vehicle) for i in range(N_cars)]

    # Other stuff
    charging_cost_per_hour = df_spotprice["Price [NOK/MWh]"] / 1000  # Make to NOK/kWh
    charging_cost_per_hour = charging_cost_per_hour.iloc[:n_hours].tolist()
    max_total_load = 500

    # Create a new model
    m = Model(env=env)

    # Decision variables
    charge_rate = m.addVars(N_cars, n_hours, lb=0, vtype=GRB.CONTINUOUS, name="charge_rate")
    peak_charge = m.addVar(lb=0, vtype=GRB.CONTINUOUS, name="peak_charge")
    peak_load_monthly = m.addVars(unique_months, lb=0, vtype=GRB.CONTINUOUS, name="peak_load_monthly")

    # Objective function
    energy_cost = quicksum(charge_rate[v, t] * charging_cost_per_hour[t-1] for v in range(N_cars) for t in range(n_hours))
    peak_cost = quicksum(peak_costs[m] * peak_load_monthly[m] for m in unique_months)

    m.setObjective(energy_cost + peak_cost, GRB.MINIMIZE)
    con_power = []

    # Constraints
    for v in range(N_cars):
        # Constraint: Lad kun opp til den nødvendige mengden før avreise
        m.addConstr(
            quicksum(charge_rate[v, t] for t in range(vehicles[v].arrival_time, vehicles[v].departure_time)) == vehicles[v].required_charge,
            name=f"charge_constraint_{v}"
        )

        for t in range(n_hours):
            # Constraint: Ikke lad utenfor tiden som bilen står parkert
            if t < vehicles[v].arrival_time or t >= vehicles[v].departure_time:
                m.addConstr(charge_rate[v, t] == 0, name=f"no_charge_outside_stay_{v}_{t}")
            # Constraint: Ikke overskrid maksimal ladeeffekt per bil
            con_power.append(m.addConstr(charge_rate[v, t] <= vehicles[v].max_charge_rate, name=f"max_charge_rate_per_vehicle_{v}_{t}"))

    for t in range(n_hours):
        # Constraint: Ensuring the total charging at any time does not exceed the monthly peak for the corresponding month
        m.addConstr(
            quicksum(charge_rate[v, t] for v in range(N_cars)) <= peak_load_monthly[hours_to_month[t]],
            name=f"monthly_peak_{t}"
        )

        # Constraint: Ikke overskrid total ladekapasitet
        m.addConstr(
            quicksum(charge_rate[v, t] for v in range(N_cars)) <= max_total_load,
            name=f"capacity_limit"
        )

    # Sett DualReductions-parameteren til 0
    m.setParam('DualReductions', 0)
    m.setParam('Method', 0)  # For dual simplex


    torelax = con_power
    conpens = [0.1]*len(torelax)
    if power_method == "Reell":
      m.feasRelax(relaxobjtype=0, minrelax=True, vars=None, lbpen=None, ubpen=None, constrs=torelax, rhspen=conpens)

    # Optimize model
    m.optimize()

    # Display results

    if m.status == GRB.OPTIMAL:
        # Fortsett med å hente løsningsverdier
        pass
    elif m.status == GRB.INFEASIBLE:
        print("Modellen er uoverkommelig: ingen løsning funnet.")
        # Du kan bruke m.computeIIS() for å identifisere årsaken
    else:
        print(f"Optimalisering ble stoppet med status {m.status}")

    #for v in range(N_cars):
        #print(f"Vehicle {v+1} charging schedule: {[charge_rate[v, t].x for t in range(1, n_hours)]}")
        #individual_energy_cost = sum(charge_rate[v, t].x * charging_cost_per_hour[t-1] for t in range(n_hours))
        #print(f"Vehicle {v+1}  --> Energikostnad: {round(individual_energy_cost, 2)} kr")

    print("****************************")
    print("Objektiv-verdien gir: ", round(m.objVal, 2), "kr")
    print("Den totale kostnaden blir:", round(energy_cost.getValue() + peak_cost.getValue(), 2), "kr")

    # For energikostnaden må vi beregne den manuelt igjen ettersom den ikke er en egen variabel i modellen
    # total_energy_cost = sum(charge_rate[v, t].x * charging_cost_per_hour[t-1] for v in range(N_cars) for t in range(n_hours))
    print("Energikostnaden blir:", round(energy_cost.getValue(), 2), "kr")

    # Tilsvarende må vi beregne topplastkostnaden manuelt
    print("Topplastkostnaden blir:", round(peak_cost.getValue(), 2), "kr")

    # Månedlig peak load
    monthly_peak_loads = [peak_load_monthly[m].x for m in unique_months]
    print("Månedlig", monthly_peak_loads)



    def _get_optimization_results(N_cars, model, energy_cost, peak_cost, peak_load_monthly, charge_rate, charging_cost_per_hour):
        """
        Function to attrieve the relevant information from the optimizatin model and save for further analysis.

        :param model: The optimization model
        :param energy_cost: The energy cost (coming from the spot price)
        :param peak_cost: The peak cost (coming from the peak tariff)
        :param peak_load_monthly: The monthly peak loads
        :param charge_rate:
        :param charging_cost_per_hour:
        :return:
        """
        # Get the total load
        total_load_profile = [sum(charge_rate[v, t].X for v in range(N_cars)) for t in range(n_hours)]

        # Get the total costs
        exceeded_power = round(model.objVal,2)  # Objektivverdien gir hvor mye man har overskridet effekt-begrensningen for biler
        total_energy_cost = round(energy_cost.getValue(),2)  # Energy costs
        total_peak_cost = round(peak_cost.getValue(),2)  # Peak costs
        total_cost = total_energy_cost + total_peak_cost # Total cost = energy cost + peak cost

        # Get peak load and peak cost of each month
        peak_load_monthly_value = [peak_load_monthly[m].x for m in unique_months]
        peak_load_monthly_value = dict(zip(unique_months, peak_load_monthly_value))  #  Make a dict with month as key and load as value

        peak_cost_monthly = [peak_tariff[m] * peak_load_monthly_value[m] for m in unique_months]
        peak_cost_monthly = dict(zip(unique_months, peak_cost_monthly))  #  Make a dict with month as key and peak cost as value

        # The energy cost of each car
        vehicle_energy_cost = [sum(charge_rate[v, t].x * charging_cost_per_hour[t-1] for t in range(n_hours))for v in range(N_cars)]

        # =================================
        # Get
        # =================================
        vehicles_charging_schedule = {}  # The charging schedule for each car
        vehicle_total_charge = []  # The total charged energy for each car

        for v in range(N_cars):
            vehicle_charge_rates = [charge_rate[v, t].x for t in range(n_hours)]  # Todo: Denne har du også regnet ut ovenfor..
            vehicle_total_charge.append(sum(vehicle_charge_rates))

            vehicles_charging_schedule[v] = vehicle_charge_rates


        # Get monthly energy cost
        hours_to_month_copy = hours_to_month.copy()
        hours_to_month_copy.pop(list(hours_to_month_copy.keys())[-1])

        monthly_energy_charge = defaultdict(int)
        monthly_energy_cost = defaultdict(int)

        # Legg til energiforbruket  og energikostnaden for den gitte timen til den tilsvarende måneden
        for key, month in hours_to_month_copy.items():
            monthly_energy_charge[month] += total_load_profile[key]
            monthly_energy_cost[month] += total_load_profile[key] * charging_cost_per_hour[key]


        # Make dict of all the results
        export_dict = {"peak_tariff": peak_tariff, "start_date": start_time_utc.strftime('%Y-%m-%d'),
                       "total_cost": total_cost, "total_energy_cost": total_energy_cost,
                       "total_peak_cost": total_peak_cost, "exceeded_power": exceeded_power,
                       "peak_load_monthly": peak_load_monthly_value,
                       "peak_cost_monthly": peak_cost_monthly, "vehicle_energy_cost": vehicle_energy_cost,
                       "total_load_profile": total_load_profile, "vehicles_charging_schedule": vehicles_charging_schedule,
                       "monthly_energy_charge": monthly_energy_charge, "monthly_energy_cost": monthly_energy_cost}

        return export_dict

    # ========================================================
    # Save the relevant information from the optimization
    # ========================================================

    optimization_results = _get_optimization_results(N_cars, m, energy_cost, peak_cost, peak_load_monthly, charge_rate, charging_cost_per_hour)

    return optimization_results