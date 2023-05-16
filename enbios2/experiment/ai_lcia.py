import numpy as np
from numba import njit, jit
from pint import UnitRegistry

ureg = UnitRegistry()
ureg.define('DALY = []')  # Define DALY as a dimensionless unit
ureg.define('Pt = []')  # Define Pt as a dimensionless unit

from dataclasses import dataclass


# this class is currently not used.
@dataclass
class LCAData:
    techno_matrix: np.ndarray  # Technomatrix
    demand_matrix: np.ndarray  # Demand matrix
    biosphere_matrix: np.ndarray  # Biosphere matrix
    characterization_matrix: np.ndarray  # Characterization matrix
    processes: dict
    demands: dict
    environmental_exchanges: dict
    impact_categories: dict

    def quantities(self):
        # todo: implement
        A_quantities = [ureg.Quantity(1, unit) for unit in A_units]
        A_normalization = np.array([quantity.to_base_units().magnitude for quantity in A_quantities])

        pass


# Technomatrix (A)
A = np.array([[0.1, 0.2, 0.3],
              [0.4, 0.2, 0.1],
              [0.1, 0.3, 0.1]]) * ureg.kg / ureg.kg

processes = {0: {'name': 'Process 1', 'units': 'kg'},
             1: {'name': 'Process 2', 'units': 'kg'},
             2: {'name': 'Process 3', 'units': 'kg'}}

# Demand matrix (F)
F = np.array([[100, 200],
              [300, 400],
              [500, 600]]) * ureg.kg

demands = {0: {'name': 'Demand Scenario 1', 'units': 'units'},
           1: {'name': 'Demand Scenario 2', 'units': 'units'}}

# Biosphere matrix (B)
B = np.array([[0.1, 0.2, 0.3],
              [0.3, 0.2, 0.1]]) * ureg.kg / ureg.kg

environmental_exchanges = {0: {'name': 'Emission 1', 'units': 'kg'},
                           1: {'name': 'Emission 2', 'units': 'kg'}}

# Characterization matrix (C)
C = np.array([[1, 2],
              [3, 4]]) * (ureg.DALY / ureg.kg)

impact_categories = {0: {'name': 'Impact Category 1', 'units': 'DALY/kg'},
                     1: {'name': 'Impact Category 2', 'units': 'Pt/kg'}}


def even_units(A, B):
    pass

# Function to calculate LCIA
def calculate_lcia(lca_data: LCAData):

    # Convert A from Pint Quantity to numpy array, and store the units
    A_units = lca_data.techno_matrix.units
    A = lca_data.techno_matrix.magnitude

    # Calculate the product matrix (I - A)^-1
    I = np.eye(A.shape[0])
    # I = np.eye(A.shape[0]) * ureg.dimensionless

    product_matrix = np.linalg.inv(I - A) * A_units
    # product_matrix = np.linalg.inv(I - A)

    # Calculate the life cycle inventory (LCI) for each demand scenario
    G = product_matrix @ F  # because @ is the matrix multiplication operator for pint arrays
    # G = np.dot(product_matrix, F)
    # Calculate the environmental interventions for each demand scenario
    E = B @ G
    # E = np.dot(B, G)
    # Calculate the life cycle impact assessment (LCIA) for each demand scenario
    H = C @ E
    # H = np.dot(C, E)

    # Individual contributions per process and impact category per scenario
    individual_contributions = []

    for i in range(H.shape[0]):
        impact_category_contributions = []
        for j in range(H.shape[1]):
            process_contributions = G[:, j] * C[i, j]
            impact_category_contributions.append(process_contributions)
        individual_contributions.append(impact_category_contributions)

    return H, individual_contributions


def generate_random_data(num_processes, num_demand_scenarios, num_environmental_exchanges, num_impact_categories):
    # Generate random data for Technomatrix (A)
    techno_matrix = np.random.rand(num_processes, num_processes)

    # Generate random data for Demand matrix (F)
    demand_matrix = np.random.rand(num_processes, num_demand_scenarios)

    # Generate random data for Biosphere matrix (B)
    biosphere_matrix = np.random.rand(num_environmental_exchanges, num_processes)

    # Generate random data for Characterization matrix (C)
    characterization_matrix = np.random.rand(num_impact_categories, num_environmental_exchanges)
    return techno_matrix, demand_matrix, biosphere_matrix, characterization_matrix

@jit(nopython=True, parallel=True)
def numba_generate_random_data(num_processes, num_demand_scenarios, num_environmental_exchanges, num_impact_categories):
    # Generate random data for Technomatrix (A)
    A = np.random.rand(num_processes, num_processes)

    # Generate random data for Demand matrix (F)
    F = np.random.rand(num_processes, num_demand_scenarios)

    # Generate random data for Biosphere matrix (B)
    B = np.random.rand(num_environmental_exchanges, num_processes)

    # Generate random data for Characterization matrix (C)
    C = np.random.rand(num_impact_categories, num_environmental_exchanges)
    return A, F, B, C
    # return A * ureg.kg / ureg.kg, F * ureg.kg, B * ureg.kg / ureg.kg, C * (ureg.DALY / ureg.kg)


def print_random_data(A, F, B, C):
    print("Technomatrix (A):")
    print(A)
    print()

    print("Demand matrix (F):")
    print(F)
    print()

    print("Biosphere matrix (B):")
    print(B)
    print()

    print("Characterization matrix (C):")
    print(C)
    print()


def print_results(H, individual_contributions=None):
    # Print individual contributions per impact category and scenario

    if individual_contributions:
        for i in range(H.shape[0]):
            for j in range(H.shape[1]):
                print(f"Impact Category: {impact_categories[i]['name']}")
                print(f"Demand Scenario: {demands[j]['name']}")
                for k, process_contribution in enumerate(individual_contributions[i][j]):
                    print(f"Process: {processes[k]['name']}, Contribution: {process_contribution}")
                print()

    # Print LCIA results with metadata
    for i in range(H.shape[0]):
        for j in range(H.shape[1]):
            print(f"{impact_categories[i]['name']} for {demands[j]['name']}: {H[i, j]}")


num_processes = 1_000
num_demand_scenarios = 1
num_environmental_exchanges = 40
num_impact_categories = 1


# generate_random_data(num_processes, num_demand_scenarios, num_environmental_exchanges, num_impact_categories)
#
# H, individual_contributions = calculate_lcia(A, F, B, C)
#
# print_results(H, individual_contributions)


def timer(funct, *args, **kwargs):
    import time
    start = time.time()
    res = funct(*args, **kwargs)
    end = time.time()
    # to minutes/seconds
    return end - start, res


def compare_speed():
    # for te,b in ((1_000, 50), (2_000, 50), (3_000, 100), (5_000,200)):
    for te, b in ((5_000, 200), (10_000, 500), (5_000, 200)):
        print("t", te, "b", b)
        t, _ = timer(generate_random_data, te, num_demand_scenarios, b,
                       num_impact_categories)
        print("py", t)
        t, _ = timer(numba_generate_random_data, te, num_demand_scenarios, b,
                       num_impact_categories)
        print("nb", t)


t, data = timer(generate_random_data, te, num_demand_scenarios, b,
               num_impact_categories)


data.quantities()