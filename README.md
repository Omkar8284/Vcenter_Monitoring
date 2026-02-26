vCenter VM Monitoring with Ansible

Automated VMware vCenter VM data collection using Ansible.
Generates daily JSON reports for auditing, monitoring, and infrastructure visibility.

▶ Run Playbook
ansible-playbook /etc/ansible/playbooks/vcenter_monitoring.yml -i "localhost,"
✔ Verify Collected VM Count
cat /etc/ansible/reports/vcenter_vms_$(date +%F).json | grep '"device_ID"' | wc -l
🔐 Requirement

Read-only vCenter user access

Ansible installed on control node

📊 Output

JSON report generated in:

/etc/ansible/reports/vcenter_vms_<date>.json

Lightweight • Production-ready • Monitoring-friendly
