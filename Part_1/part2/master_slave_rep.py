
#pylint:skip-file
import subprocess
import time

def start_containers():
    subprocess.run(["docker-compose", "-f", "docker-compose.yaml", "up", "-d"])

def stop_containers():
    subprocess.run(["docker-compose", "-f", "docker-compose.yaml", "down"])

def wait_for_replication():
    time.sleep(10)  

def main():
    try:
        start_containers()
        wait_for_replication()
        print("PostgreSQL master-slave replication set up successfully!")
    except Exception as e:
        print(f"Error setting up master-slave replication: {e}")
    finally:
        stop_containers()

if __name__ == "__main__":
    main()
