class SimulationExists(Exception):
    def __init__(self, msg, sim_id, sim_dir):
        super().__init__(msg)
        self.sim_id = sim_id
        self.sim_dir = sim_dir


class ValidationError(Exception):
    pass
