"""
OEE Sample Data Generator
Creates and populates four tables (machine, production, runtime, downtime)
with realistic OEE data for 6 machines over 24 hours.
Includes edge cases: zero good count, high planned downtime, performance bottlenecks.

Converted from Ignition Jython to standalone Python 3.
"""

import pyodbc
from datetime import datetime, timedelta
import sys

# Database connection settings
# You can modify these or pass via command line / environment variables
DB_SERVER = "20.127.233.148"
DB_DATABASE = "model"  # Change this to your target database
DB_USERNAME = "leor"  # Fill in your username
DB_PASSWORD = "leortest1!!!"  # Fill in your password

# Global connection object
_connection = None


def get_connection():
    """Get or create database connection."""
    global _connection
    if _connection is None:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_DATABASE};"
            f"UID={DB_USERNAME};"
            f"PWD={DB_PASSWORD}"
        )
        _connection = pyodbc.connect(conn_str)
        _connection.autocommit = True
    return _connection


def run_update_query(query, params=None):
    """Execute an update query (INSERT, UPDATE, DELETE, CREATE, DROP)."""
    conn = get_connection()
    cursor = conn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    cursor.close()


def createTables():
    """
    Creates the four OEE sample data tables: machine, production, runtime, downtime
    """
    try:
        # Drop existing tables if they exist
        dropQueries = [
            "IF OBJECT_ID('downtime', 'U') IS NOT NULL DROP TABLE downtime",
            "IF OBJECT_ID('production', 'U') IS NOT NULL DROP TABLE production",
            "IF OBJECT_ID('runtime', 'U') IS NOT NULL DROP TABLE runtime",
            "IF OBJECT_ID('machine', 'U') IS NOT NULL DROP TABLE machine",
        ]

        for query in dropQueries:
            run_update_query(query)

        # Create machine table
        createMachineTable = """
        CREATE TABLE machine (
            machine_id INT PRIMARY KEY,
            machine_name VARCHAR(50) NOT NULL,
            ideal_cycle_time DECIMAL(10,2) NOT NULL,
            description VARCHAR(200)
        )
        """
        run_update_query(createMachineTable)

        # Create production table
        createProductionTable = """
        CREATE TABLE production (
            production_id INT IDENTITY(1,1) PRIMARY KEY,
            machine_id INT NOT NULL,
            timestamp DATETIME NOT NULL,
            good_count INT NOT NULL,
            reject_count INT NOT NULL,
            total_count INT NOT NULL,
            FOREIGN KEY (machine_id) REFERENCES machine(machine_id)
        )
        """
        run_update_query(createProductionTable)

        # Create runtime table
        createRuntimeTable = """
        CREATE TABLE runtime (
            runtime_id INT IDENTITY(1,1) PRIMARY KEY,
            machine_id INT NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME NOT NULL,
            duration_minutes INT NOT NULL,
            FOREIGN KEY (machine_id) REFERENCES machine(machine_id)
        )
        """
        run_update_query(createRuntimeTable)

        # Create downtime table
        createDowntimeTable = """
        CREATE TABLE downtime (
            downtime_id INT IDENTITY(1,1) PRIMARY KEY,
            machine_id INT NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME NOT NULL,
            duration_minutes INT NOT NULL,
            downtime_type VARCHAR(50) NOT NULL,
            reason VARCHAR(200),
            FOREIGN KEY (machine_id) REFERENCES machine(machine_id)
        )
        """
        run_update_query(createDowntimeTable)

        return True
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False


def populateMachineData():
    """
    Populates the machine table with 6 machines with varying characteristics
    """
    try:
        machines = [
            (1, "Assembly Line 1", 12.0, "High-speed assembly line"),
            (2, "Packaging Unit A", 8.5, "Primary packaging unit"),
            (3, "CNC Mill 3", 45.0, "Precision milling machine"),
            (4, "Injection Mold 2", 35.0, "Plastic injection molding"),
            (5, "Quality Check Station", 5.0, "Automated quality inspection"),
            (6, "Bottleneck Press", 60.0, "Performance bottleneck machine"),
        ]

        insertQuery = """
        INSERT INTO machine (machine_id, machine_name, ideal_cycle_time, description)
        VALUES (?, ?, ?, ?)
        """

        for machine in machines:
            run_update_query(
                insertQuery, [machine[0], machine[1], machine[2], machine[3]]
            )

        return True
    except Exception as e:
        print(f"Error populating machine data: {e}")
        return False


def getTimestamp(hoursOffset, minutesOffset=0):
    """
    Helper function to generate timestamps relative to now
    """
    return datetime.now() + timedelta(hours=hoursOffset, minutes=minutesOffset)


def populateProductionData():
    """
    Populates production table with hourly data for each machine over 24 hours
    Includes edge cases: zero good count, varying quality levels
    """
    try:
        # Production data for each machine (hourly good_count, reject_count patterns)
        # Machine 1: Normal operation with occasional quality issues
        machine1Data = [
            (450, 15),
            (460, 18),
            (455, 12),
            (470, 20),
            (465, 16),
            (458, 14),
            (462, 17),
            (468, 19),
            (455, 15),
            (460, 18),
            (465, 16),
            (470, 22),
            (458, 14),
            (462, 17),
            (468, 19),
            (472, 21),
            (460, 18),
            (465, 16),
            (470, 20),
            (458, 14),
            (462, 17),
            (468, 19),
            (465, 16),
            (470, 20),
        ]

        # Machine 2: Good performance, then quality issue spike
        machine2Data = [
            (680, 8),
            (685, 10),
            (690, 9),
            (675, 7),
            (680, 8),
            (688, 11),
            (682, 9),
            (690, 10),
            (685, 8),
            (680, 7),
            (0, 0),
            (0, 0),  # Zero production hours
            (320, 45),
            (340, 52),
            (335, 48),
            (685, 9),
            (680, 8),
            (688, 11),
            (682, 9),
            (690, 10),
            (685, 8),
            (680, 7),
            (688, 11),
            (685, 9),
        ]

        # Machine 3: Slower cycle, consistent quality
        machine3Data = [
            (125, 3),
            (128, 4),
            (127, 3),
            (126, 2),
            (129, 4),
            (128, 3),
            (127, 3),
            (126, 2),
            (128, 4),
            (125, 3),
            (127, 3),
            (129, 4),
            (126, 2),
            (128, 3),
            (127, 3),
            (125, 2),
            (129, 4),
            (128, 3),
            (127, 3),
            (126, 2),
            (128, 4),
            (125, 3),
            (127, 3),
            (129, 4),
        ]

        # Machine 4: Variable production with quality concerns
        machine4Data = [
            (165, 8),
            (168, 10),
            (170, 9),
            (172, 12),
            (165, 8),
            (168, 10),
            (170, 11),
            (172, 13),
            (165, 8),
            (168, 10),
            (170, 9),
            (172, 12),
            (165, 8),
            (168, 10),
            (85, 45),
            (90, 50),
            (165, 8),
            (168, 10),  # Quality drop
            (170, 11),
            (172, 12),
            (165, 8),
            (168, 10),
            (170, 9),
            (172, 12),
        ]

        # Machine 5: Fast cycle, high volume
        machine5Data = [
            (1150, 25),
            (1160, 28),
            (1155, 22),
            (1170, 30),
            (1165, 26),
            (1158, 24),
            (1162, 27),
            (1168, 29),
            (1155, 25),
            (1160, 28),
            (1165, 26),
            (1170, 32),
            (1158, 24),
            (1162, 27),
            (1168, 29),
            (1172, 31),
            (1160, 28),
            (1165, 26),
            (1170, 30),
            (1158, 24),
            (1162, 27),
            (1168, 29),
            (1165, 26),
            (1170, 30),
        ]

        # Machine 6: Performance bottleneck - slower than ideal
        machine6Data = [
            (85, 5),
            (82, 6),
            (88, 4),
            (80, 5),
            (87, 6),
            (85, 5),
            (83, 4),
            (86, 5),
            (84, 6),
            (85, 5),
            (82, 4),
            (88, 6),
            (80, 5),
            (87, 4),
            (85, 5),
            (83, 6),
            (86, 4),
            (84, 5),
            (85, 6),
            (82, 4),
            (88, 5),
            (80, 6),
            (87, 5),
            (85, 4),
        ]

        allMachineData = [
            machine1Data,
            machine2Data,
            machine3Data,
            machine4Data,
            machine5Data,
            machine6Data,
        ]

        insertQuery = """
        INSERT INTO production (machine_id, timestamp, good_count, reject_count, total_count)
        VALUES (?, ?, ?, ?, ?)
        """

        for machineId in range(1, 7):
            machineData = allMachineData[machineId - 1]
            for hour in range(24):
                goodCount = machineData[hour][0]
                rejectCount = machineData[hour][1]
                totalCount = goodCount + rejectCount
                timestamp = getTimestamp(-24 + hour, 30)  # Set to half-past each hour

                run_update_query(
                    insertQuery,
                    [machineId, timestamp, goodCount, rejectCount, totalCount],
                )

        return True
    except Exception as e:
        print(f"Error populating production data: {e}")
        return False


def populateRuntimeData():
    """
    Populates runtime table with operating periods for each machine
    """
    try:
        insertQuery = """
        INSERT INTO runtime (machine_id, start_time, end_time, duration_minutes)
        VALUES (?, ?, ?, ?)
        """

        # Machine 1: Normal 3 shift operation
        runtimes = [
            (1, -24, 0, -22, 0, 120),  # 2 hour run
            (1, -22, 0, -14, 0, 480),  # 8 hour run
            (1, -14, 0, -6, 0, 480),  # 8 hour run
            (1, -6, 0, 0, 0, 360),  # 6 hour run
        ]

        # Machine 2: Operation with significant downtime
        runtimes.extend(
            [
                (2, -24, 0, -14, 0, 600),  # 10 hour run
                (2, -12, 0, 0, 0, 720),  # 12 hour run (after 2-hour downtime)
            ]
        )

        # Machine 3: Consistent operation
        runtimes.extend(
            [
                (3, -24, 0, -16, 0, 480),  # 8 hour run
                (3, -16, 0, -8, 0, 480),  # 8 hour run
                (3, -8, 0, 0, 0, 480),  # 8 hour run
            ]
        )

        # Machine 4: Multiple short runs
        runtimes.extend(
            [
                (4, -24, 0, -20, 0, 240),  # 4 hour run
                (4, -20, 0, -16, 0, 240),  # 4 hour run
                (4, -16, 0, -12, 0, 240),  # 4 hour run
                (4, -12, 0, -8, 0, 240),  # 4 hour run
                (4, -8, 0, -4, 0, 240),  # 4 hour run
                (4, -4, 0, 0, 0, 240),  # 4 hour run
            ]
        )

        # Machine 5: Nearly continuous operation
        runtimes.extend(
            [
                (5, -24, 0, -12, 0, 720),  # 12 hour run
                (5, -11, 30, 0, 0, 690),  # 11.5 hour run (after 30-min break)
            ]
        )

        # Machine 6: Heavy planned downtime, short runs
        runtimes.extend(
            [
                (6, -24, 0, -20, 0, 240),  # 4 hour run
                (6, -14, 0, -10, 0, 240),  # 4 hour run (after 6-hour downtime)
                (6, -8, 0, -4, 0, 240),  # 4 hour run (after 2-hour downtime)
                (6, -2, 0, 0, 0, 120),  # 2 hour run (after 2-hour downtime)
            ]
        )

        for runtime in runtimes:
            machineId = runtime[0]
            startTime = getTimestamp(runtime[1], runtime[2])
            endTime = getTimestamp(runtime[3], runtime[4])
            duration = runtime[5]

            run_update_query(insertQuery, [machineId, startTime, endTime, duration])

        return True
    except Exception as e:
        print(f"Error populating runtime data: {e}")
        return False


def populateDowntimeData():
    """
    Populates downtime table with various downtime types and reasons
    Includes planned maintenance, unplanned breakdowns, and changeovers
    """
    try:
        insertQuery = """
        INSERT INTO downtime (machine_id, start_time, end_time, duration_minutes, downtime_type, reason)
        VALUES (?, ?, ?, ?, ?, ?)
        """

        downtimes = [
            # Machine 1: Minor stoppages
            (1, -22, 0, -22, 30, 30, "Unplanned", "Material jam"),
            (1, -14, 0, -14, 30, 30, "Planned", "Shift changeover"),
            (1, -6, 0, -6, 30, 30, "Planned", "Shift changeover"),
            # Machine 2: Extended downtime for repairs
            (
                2,
                -14,
                0,
                -12,
                0,
                120,
                "Unplanned",
                "Equipment failure - motor replacement",
            ),
            # Machine 3: Regular maintenance
            (3, -16, 0, -16, 30, 30, "Planned", "Routine maintenance"),
            (3, -8, 0, -8, 30, 30, "Planned", "Tool change"),
            # Machine 4: No downtime recorded (already accounted in runtime gaps)
            # Machine 5: Minimal downtime
            (5, -12, 0, -11, 30, 30, "Planned", "Lunch break"),
            # Machine 6: Heavy planned downtime (performance bottleneck)
            (6, -20, 0, -14, 0, 360, "Planned", "Extended preventive maintenance"),
            (6, -10, 0, -8, 0, 120, "Planned", "Setup and changeover"),
            (6, -4, 0, -2, 0, 120, "Unplanned", "Quality inspection failure"),
        ]

        for downtime in downtimes:
            machineId = downtime[0]
            startTime = getTimestamp(downtime[1], downtime[2])
            endTime = getTimestamp(downtime[3], downtime[4])
            duration = downtime[5]
            downtimeType = downtime[6]
            reason = downtime[7]

            run_update_query(
                insertQuery,
                [machineId, startTime, endTime, duration, downtimeType, reason],
            )

        return True
    except Exception as e:
        print(f"Error populating downtime data: {e}")
        return False


def generateAllData():
    """
    Main function to create tables and populate all OEE sample data
    """
    try:
        print("Starting OEE sample data generation...")

        # Create tables
        if not createTables():
            print("Failed to create tables")
            return False
        print("Tables created successfully")

        # Populate machine data
        if not populateMachineData():
            print("Failed to populate machine data")
            return False
        print("Machine data populated successfully")

        # Populate production data
        if not populateProductionData():
            print("Failed to populate production data")
            return False
        print("Production data populated successfully")

        # Populate runtime data
        if not populateRuntimeData():
            print("Failed to populate runtime data")
            return False
        print("Runtime data populated successfully")

        # Populate downtime data
        if not populateDowntimeData():
            print("Failed to populate downtime data")
            return False
        print("Downtime data populated successfully")

        print("OEE sample data generation complete!")
        return True

    except Exception as e:
        print(f"Error in generateAllData: {e}")
        return False
    finally:
        # Close connection when done
        global _connection
        if _connection:
            _connection.close()
            _connection = None


if __name__ == "__main__":
    # Allow passing credentials via command line args
    if len(sys.argv) >= 3:
        DB_USERNAME = sys.argv[1]
        DB_PASSWORD = sys.argv[2]
    if len(sys.argv) >= 4:
        DB_DATABASE = sys.argv[3]

    if not DB_USERNAME or not DB_PASSWORD:
        print("Usage: python oee_sample_data.py <username> <password> [database]")
        print("  database defaults to 'master' if not specified")
        sys.exit(1)

    generateAllData()
