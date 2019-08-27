from pyomo.opt import SolverFactory
from pyomo.environ import *
import pandas

def maximize_self_consumption(uncontrollable, dfbatteries,
                              dfshapeables, dfdeferrables,
                              timestep, solver='gurobi',
                              verbose=False, solver_path=None,
                              timelimit=5*60):
    """
    Version v001 Minimize \sum_{t}^T peak^+ - peak^-
    Optimize batteries, shapeable and deferrable loads to maximize
    collective self-consumption.
    Inputs:
        - uncontrollable (DataFrame): uncontrollable load demand
        - dfbatteries (DataFrame): order book
        - dfshapeables (DataFrame): order book
        - dfdeferrables (DataFrame): order book
        - timestep (float): one is equivalent to hourly timestep
    Outputs:
        - demandshape
        - batteryin
        - batteryout
        - batteryenergy
        - demanddeferr
        - deferrschedule
        - demand_controllable
        - community_import
        - total community_import
        - peakhigh
        - peaklow
    """
    # Inputs
    horizon = uncontrollable.index.tolist()
    demand_uncontrollable = uncontrollable.p.to_list()
    batteries = dfbatteries.index.tolist()
    shapeables = dfshapeables.index.tolist()
    deferrables = dfdeferrables.index.tolist()
    m = ConcreteModel()

    ###################################################### Set
    m.horizon = Set(initialize=horizon, ordered=True)
    last = m.horizon.last()
    m.batteries = Set(initialize=batteries, ordered=True)
    m.shapeables = Set(initialize=shapeables, ordered=True)
    m.deferrables = Set(initialize=deferrables, ordered=True)

    ##################################################### Var
    m.demand_controllable = Var(m.horizon, domain=Reals)
    m.peakhigh = Var(domain=Reals)
    m.peaklow = Var(domain=Reals)

    # Equipment specifics
    m.demandshape = Var(m.horizon, m.shapeables, domain=Reals)
    m.batteryin = Var(m.horizon, m.batteries, domain=Reals)
    m.batteryout = Var(m.horizon, m.batteries, domain=Reals)
    m.batteryenergy = Var(m.horizon, m.batteries, domain=Reals)
    m.demanddeferr = Var(m.horizon, m.deferrables, domain=Reals)
    m.deferrschedule = Var(m.horizon, m.deferrables,
                           domain=NonNegativeIntegers)

    #################################################### Rules
    # --------------------------------------------------------
    # ------------------shapeable load------------------------
    # --------------------------------------------------------
    # The power bounds are defined by the load characteristics
    def r_shape_min_power(m, t, s):
        return (m.demandshape[t, s] >= 0)

    def r_shape_max_power(m, t, s):
        return (m.demandshape[t, s] <=
                dfshapeables.loc[s, 'max_kw'])

    # At the end the energy asked by the load is satisfied
    def r_shape_energy(m, s):
        return (sum(m.demandshape[i, s] for i in m.horizon) * timestep ==
                dfshapeables.loc[s, 'end_kwh'])

    # If we are outside of startby - endby, we enforce zero power
    def r_shape_timebounds(m, t, s):
        if t < dfshapeables.loc[s, 'startby']:
            return m.demandshape[t, s] == 0
        if t > dfshapeables.loc[s, 'endby']:
            return m.demandshape[t, s] == 0
        else:
            return Constraint.Skip

    # --------------------------------------------------------
    # ---------------------Battery----------------------------
    # --------------------------------------------------------
    # The power bounds are defined by the battery characteristics
    def r_battery_min_powerin(m, t, b):
        return (m.batteryin[t, b] >= 0)

    def r_battery_max_powerin(m, t, b):
        return (m.batteryin[t, b] <= dfbatteries.loc[b, 'max_kw'])

    def r_battery_min_powerout(m, t, b):
        return (m.batteryout[t, b] >= 0)

    def r_battery_max_powerout(m, t, b):
        return (m.batteryout[t, b] <= dfbatteries.loc[b, 'min_kw'])

    # Define the SOC considering charge/discharge efficiency
    def r_battery_energy(m, t, b):
        if t == 0:
            return m.batteryenergy[t, b] == dfbatteries.loc[b, 'initial_kwh']
        else:
            return (m.batteryenergy[t, b] ==
                    m.batteryenergy[t-1, b] +
                    m.batteryin[t, b] * timestep * dfbatteries.loc[b, 'eta']
                    - m.batteryout[t, b] * timestep / dfbatteries.loc[b, 'eta'])
                    # 0.25 pour un quart d'heure

    # Energy bound during operation
    def r_battery_min_energy(m, t, b):
        return (m.batteryenergy[t, b] >= 0)

    def r_battery_max_energy(m, t, b):
        return (m.batteryenergy[t, b] <= dfbatteries.loc[b, 'max_kwh'])

    # Energy status at the end
    def r_battery_end_energy(m, b):
        return (m.batteryenergy[last, b] >= dfbatteries.loc[b, 'end_kwh'])

    # If we are outside of startby - endby, we enforce no operation
    def r_batteryin_timebounds(m, t, b):
        if t < dfbatteries.loc[b, 'startby']:
            return (m.batteryin[t, b] == 0)
        if t > dfbatteries.loc[b, 'endby']:
            return (m.batteryin[t, b] == 0)
        else:
            return Constraint.Skip

    def r_batteryout_timebounds(m, t, b):
        if t < dfbatteries.loc[b, 'startby']:
            return (m.batteryout[t, b] == 0)
        if t > dfbatteries.loc[b, 'endby']:
            return (m.batteryout[t, b] == 0)
        else:
            return Constraint.Skip

    # --------------------------------------------------------
    # ---------------------Deferrable-------------------------
    # --------------------------------------------------------
    # Convolution of the power profile (time horizon L)
    # and the scheduler (time horizon T)
    def r_deferrable_schedule(m, t, d):
        return (m.demanddeferr[t, d] ==
                sum(m.deferrschedule[t - k, d] * dfdeferrables.loc[d, 'profile_kw'][k]
                   for k in range(0, min(dfdeferrables.loc[d, 'duration'], t + 1))))

    # We can only schedule a load once within the time horizon
    def r_deferrable_schedule_sum(m, d):
        return (sum(m.deferrschedule[i, d] for i in m.horizon) == 1)

    # If we are outside of startby - endby, we enforce no operation
    def r_deferrable_timebounds(m, t, d):
        if t < dfdeferrables.loc[d, 'startby']:
            return (m.demanddeferr[t, d] == 0)
        if t > dfdeferrables.loc[d, 'endby']:
            return (m.demanddeferr[t, d] == 0)
        else:
            return Constraint.Skip

    # --------------------------------------------------------
    # ---------------------Helper-----------------------------
    # --------------------------------------------------------
    # Useless step which seems to be necessary
    def r_demand_total(m, t):
        return (m.demand_controllable[t] ==
                sum(m.demandshape[t, s] for s in m.shapeables) +
                sum(m.batteryin[t, b] - m.batteryout[t, b] for b in m.batteries) +
                sum(m.demanddeferr[t, d] for d in m.deferrables))

    # Limit maximum peak
    def r_peak_high(m, t):
        return (m.demand_controllable[t] + demand_uncontrollable[t]
                <= m.peakhigh)

    # Stop constraint at 0
    def r_peak_high_zero(m, t):
        return (0 <= m.peakhigh)

    # Limit minimum peak
    def r_peak_low(m, t):
        return (m.peaklow
                <= m.demand_controllable[t] + demand_uncontrollable[t])

    # Stop constraint at 0
    def r_peak_low_zero(m, t):
        return (m.peaklow <= 0)

    # Shapeable
    m.r1 = Constraint(m.horizon, m.shapeables, rule=r_shape_min_power)
    m.r2 = Constraint(m.horizon, m.shapeables, rule=r_shape_max_power)
    m.r3 = Constraint(m.shapeables, rule=r_shape_energy)
    m.r4 = Constraint(m.horizon, m.shapeables, rule=r_shape_timebounds)
    # Battery
    m.r5 = Constraint(m.horizon, m.batteries, rule=r_battery_min_powerin)
    m.r6 = Constraint(m.horizon, m.batteries, rule=r_battery_max_powerin)
    m.r7 = Constraint(m.horizon, m.batteries, rule=r_battery_min_powerout)
    m.r8 = Constraint(m.horizon, m.batteries, rule=r_battery_max_powerout)
    m.r9 = Constraint(m.horizon, m.batteries, rule=r_battery_energy)
    m.r10 = Constraint(m.horizon, m.batteries, rule=r_battery_min_energy)
    m.r11 = Constraint(m.horizon, m.batteries, rule=r_battery_max_energy)
    m.r12 = Constraint(m.batteries, rule=r_battery_end_energy)
    m.r13 = Constraint(m.horizon, m.batteries, rule=r_batteryin_timebounds)
    m.r14 = Constraint(m.horizon, m.batteries, rule=r_batteryout_timebounds)
    # Deferrable
    m.r16 = Constraint(m.horizon, m.deferrables, rule=r_deferrable_schedule)
    m.r17 = Constraint(m.deferrables, rule=r_deferrable_schedule_sum)
    m.r18 = Constraint(m.horizon, m.deferrables, rule=r_deferrable_timebounds)
    # Helper
    m.r15 = Constraint(m.horizon, rule=r_demand_total)
    m.r19 = Constraint(m.horizon, rule=r_peak_high)
    m.r20 = Constraint(m.horizon, rule=r_peak_low)
    m.r21 = Constraint(m.horizon, rule=r_peak_low_zero)
    m.r22 = Constraint(m.horizon, rule=r_peak_high_zero)

    ##################################################### Objective function
    # Linear objective function
    def objective_function(m):
        return (m.peakhigh - m.peaklow)
    m.objective = Objective(rule=objective_function, sense=minimize)

    #################################################### Run
    # Solve optimization problem
    with SolverFactory(solver, executable=solver_path) as opt:
        if solver in 'glpk':
            opt.options['tmlim'] = timelimit
            results = opt.solve(m, tee=verbose)
        if solver in 'gurobi':
            opt.options['TimeLimit'] = timelimit
            results = opt.solve(m, tee=verbose)
        if solver in 'cbc':
            results = opt.solve(m, timelimit=timelimit, tee=verbose)
#         else:
#             results = opt.solve(m, tee=verbose)

    if verbose:
        print(results)

    #################################################### Results
    # A Dictionnary contains all the results
    results = {}
    keys = ['demandshape', 'batteryin',
            'batteryout', 'batteryenergy',
            'demanddeferr', 'deferrschedule']

    for key in keys:
        try:
            tmp = pandas.DataFrame(index=['none'],
                    data=getattr(m, key).get_values())
            tmp = tmp.transpose()
            tmp = tmp.unstack(level=1)
            tmp.columns = tmp.columns.levels[1]
            results[key] = tmp.copy()
        except:
            results[key] = None

    # demand_controllable
    results['demand_controllable'] = list(
        m.demand_controllable.get_values().values())

    # community_import
    results['community_import'] = [ max(0, a + b)
                                  for a, b in zip(
                                  demand_uncontrollable,
                                  results['demand_controllable'])]

    # High peak
    results['peakhigh'] = m.peakhigh.get_values()[None]

    # Low peak
    results['peaklow'] = m.peaklow.get_values()[None]

    # Total import from the community
    results['total_community_import'] = sum(
        results['community_import'] ) * timestep
    return results
