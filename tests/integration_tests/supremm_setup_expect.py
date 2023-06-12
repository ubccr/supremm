import pexpect
import sys

def main():
    scriptsettings = ['start', 'start', 'start', 'end', 'submit']
    with open("supremm_expect_log", "wb") as f:
        p = pexpect.spawn('supremm-setup')
        p.logfile = f
        
        p.expect("Select an option")
        p.sendline("c")
        
        p.expect("Enter path to configuration files")
        p.sendline()
        p.expect("Do you wish to specify the XDMoD install directory")
        p.sendline("n")
        p.expect("XDMoD mysqldb hostname")
        p.sendline()
        p.expect("XDMoD mysqldb port number")
        p.sendline()
        p.expect("XDMoD mysqldb username")
        p.sendline("xdmod")
        p.expect("XDMoD mysqldb password")
        p.sendline("xdmod123")
        p.expect("Location of my.cnf file \(where the username and passsword will be stored\)")
        p.sendline()
        p.expect("MongoDB URI")
        p.sendline("mongodb://supremm:supremm-test123@localhost:27017/supremm")
        p.expect("MongoDB database name")
        p.sendline()
        p.expect("Temporary directory to use for job archive processing")
        p.sendline()

        while True:
            i = p.expect(["Overwrite config file", "pcp_cluster", "prom_cluster"])
            if i > 1:
                p.expect('Enable SUPReMM summarization for this resource?')
            p.sendline("y")
            p.expect("Data collector backend (pcp or prometheus)")
            if i == 1:
                p.sendline()
                p.expect("Directory containing node-level PCP archives")
                p.sendline()
            elif i == 2:
                p.sendline("prometheus")
                p.expect("Hostname for Prometheus server")
                p.sendline()
                p.expect("Username for basic authentication to Prometheus server \(enter [space] for none\)")
                p.sendline(" ")
                #p.expect("Password for basic authentication to Prometheus server")
            p.expect("Source of accounting data")
            p.sendline()
            p.expect("node name unique identifier")
            p.sendline()
            p.expect("Directory containing job launch scripts")
            p.sendline()
            p.expect("Job launch script timestamp lookup mode \('submit', 'start' or 'none'\)")
            p.sendline()

        p.expect("Press ENTER to continue")
        p.sendline()

        p.expect("Select an option")
        p.sendline("d")
        p.expect("Enter path to configuration files")
        p.sendline()
        p.expect("DB hostname")
        p.sendline()
        p.expect("DB port")
        p.sendline()
        p.expect("DB Admin Username")
        p.sendline("xdmod")
        p.expect("DB Admin Password")
        p.sendline("xdmod123")
        p.expect("Do you wish to proceed")
        p.sendline("y")
        p.expect("Press ENTER to continue")
        p.sendline()

        p.expect("Select an option")
        p.sendline("m")
        p.expect("Enter path to configuration files")
        p.sendline()
        p.expect("URI")
        p.sendline("mongodb://supremm:supremm-test123@localhost:27017")
        p.expect("Do you wish to proceed")
        p.sendline("y")
        p.expect("Press ENTER to continue")
        p.sendline()
        
        p.expect("Select an option")
        p.sendline("q")

if __name__ == '__main__':
    main()
