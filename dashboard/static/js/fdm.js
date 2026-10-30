(() => {
  const labels = {
  overview: 'Cluster Overview',
  producers: 'Producer Control Center',
  snowflake: 'Snowflake Integration',
  consumers: 'Consumer Operations',
  crud: 'CRUD Playground',
  topics: 'Topic Explorer',
  failover: 'Fault Tolerance Lab',
  events: 'Live Event Monitor',
  retries: 'Retransmission Lab'
};
  function navigate(page) {
    if (!labels[page] || !document.querySelector(`[data-fdm-view="${page}"]`)) return;
    document.querySelectorAll('[data-fdm-view]').forEach(el => { el.classList.toggle('active',el.dataset.fdmView===page); });
    document.querySelectorAll('[data-fdm-nav]').forEach(el => { const active=el.dataset.fdmNav===page;el.classList.toggle('active',active);el.setAttribute('aria-current',active?'page':'false'); });
    const title=document.getElementById('fdm-page-title');if(title)title.textContent=labels[page];
    history.replaceState(null,'',`#${page}`);
  }
  document.addEventListener('DOMContentLoaded',()=>{
    document.querySelectorAll('[data-fdm-nav]').forEach(button=>button.addEventListener('click',()=>navigate(button.dataset.fdmNav)));
    const page=location.hash.slice(1);navigate(labels[page]?page:'overview');
    window.addEventListener('hashchange',()=>navigate(labels[location.hash.slice(1)]?location.hash.slice(1):'overview'));
  });
})();
