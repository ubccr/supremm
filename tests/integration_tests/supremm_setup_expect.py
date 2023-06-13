import pexpect
import sys

def config_pcp(p):
    p.sendline()
    p.expect("Directory containing node-level PCP archives")
    p.sendline()

def config_prometheus(p):
    p.sendline("prometheus")
    p.expect("Hostname for Prometheus server")
    p.sendline()
    p.expect("Username for basic authentication to Prometheus server")
    p.sendline(" ")
    #p.expect("Password for basic authentication to Prometheus server")

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
        p.sendline()
        p.expect("XDMoD configuration directory path")
        p.sendline()
        p.expect("Temporary directory to use for job archive processing")
        p.sendline()

        while True:
            i = p.expect(["Overwrite config file","frearson", "mortorq", "phillips", "pozidriv", "robertson", "openstack", "recex", "torx", "nutsetters"])
            if i > 1:
                p.expect('Enable SUPReMM summarization for this resource?')
            if i > 5:
                p.sendline("n")
                continue
            p.sendline("y")
            if i != 0:
                p.expect("Data collector backend \(pcp or prometheus\)")
                if i <= 4: 
                    config_pcp(p)
                elif i == 5:
                    config_prometheus(p)
                p.expect("Source of accounting data")
                p.sendline()
                p.expect("node name unique identifier")
                p.sendline()
                p.expect("Directory containing job launch scripts")
                p.sendline()
                p.expect("Job launch script timestamp lookup mode \('submit', 'start' or 'none'\)")
                p.sendline(scriptsettings[i-1])
            else:
                break

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
        p.sendline("mongodb://supremm:supremm-test123@mongo:27017/supremm?authSource=auth")
        p.expect("Do you wish to proceed")
        p.sendline("y")
        p.expect("Press ENTER to continue")
        p.sendline()
        
        p.expect("Select an option")
        p.sendline("q")

if __name__ == '__main__':
    main()
