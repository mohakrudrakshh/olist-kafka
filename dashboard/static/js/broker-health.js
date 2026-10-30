(() => {
  const root = document.getElementById('fdm-broker-health');
  if (!root) return;
  const cards = document.getElementById('fdm-broker-cards');
  const summary = document.getElementById('fdm-broker-summary');
  const affected = document.getElementById('fdm-broker-affected');
  const updated = document.getElementById('fdm-broker-updated');
  const add = (parent, tag, text, cls) => { const el=document.createElement(tag); el.textContent=String(text); if(cls)el.className=cls; parent.appendChild(el); return el; };
  async function refresh() {
    try {
      const response = await fetch('/api/brokers/health', {cache:'no-store'});
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Kafka health unavailable');
      cards.replaceChildren();
      data.brokers.forEach(b => {
        const card=add(cards,'div','', 'fdm-broker-card');card.textContent='';
        const heading=add(card,'div',`BROKER ${b.id}`,'fdm-broker-heading');
        add(heading,'span',b.registered?'REGISTERED':'NOT REGISTERED',b.registered?'fdm-health-good':'fdm-health-bad');
        add(card,'div',b.registered?`${b.host}:${b.port}`:'Not present in cluster metadata','fdm-broker-host');
        const stats=add(card,'div','', 'fdm-broker-stats');stats.textContent='';
        add(stats,'span',`Leaders: ${b.leader_partitions}`);
        add(stats,'span',`Replicas: ${b.replica_partitions}`);
        add(stats,'span',`In ISR: ${b.in_sync_partitions}`);
      });
      summary.replaceChildren();
      add(summary,'strong',data.healthy?'HEALTHY': 'DEGRADED',data.healthy?'fdm-health-good':'fdm-health-bad');
      add(summary,'span',`${data.registered_brokers}/${data.expected_brokers} brokers registered`);
      add(summary,'span',`${data.under_replicated_partitions} under-replicated partitions`);
      add(summary,'span',`${data.unavailable_partitions} partitions without leaders`);
      affected.replaceChildren();
      if (!data.affected_partitions.length) add(affected,'p','All monitored Olist partitions are fully replicated.');
      else data.affected_partitions.forEach(p=>add(affected,'p',`${p.topic} / partition ${p.partition} · leader ${p.leader} · ISR [${p.isr.join(', ')}] / replicas [${p.replicas.join(', ')}]`));
      updated.textContent=`Updated ${new Date().toLocaleTimeString()} · Kafka metadata`;
    } catch (err) {
      summary.textContent='HEALTH DATA UNAVAILABLE';
      affected.textContent=err.message;
      updated.textContent='Unable to refresh Kafka metadata';
    }
  }
  document.getElementById('fdm-refresh-brokers').addEventListener('click',refresh);
  refresh();
  setInterval(refresh,5000);
})();
