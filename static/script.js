let nodesData = [];
let trafficChart = null;

const ws = new WebSocket(`ws://${location.host}/ws`);
ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === 'stats') {
        nodesData = msg.data;
        updateTable();
        updateChart();
    }
};

async function loadTopology() {
    const res = await fetch('/api/topology');
    const topo = await res.json();
    drawGraph(topo.nodes, topo.links);
    populateSelects(topo.nodes);
}

function drawGraph(nodes, links) {
    const width = document.getElementById('graph').clientWidth;
    const height = 400;
    const svg = d3.select("#graph")
        .append("svg")
        .attr("width", width)
        .attr("height", height)
        .attr("viewBox", [0, 0, width, height]);

    const simulation = d3.forceSimulation(nodes)
        .force("link", d3.forceLink(links).id(d => d.id).distance(120))
        .force("charge", d3.forceManyBody().strength(-300))
        .force("center", d3.forceCenter(width/2, height/2));

    const link = svg.append("g")
        .attr("stroke", "#7a8faa")
        .attr("stroke-width", 2)
        .selectAll("line")
        .data(links)
        .join("line");

    const node = svg.append("g")
        .selectAll("circle")
        .data(nodes)
        .join("circle")
        .attr("r", 20)
        .attr("fill", "#3a86ff")
        .call(drag(simulation));

    const label = svg.append("g")
        .selectAll("text")
        .data(nodes)
        .join("text")
        .text(d => d.name)
        .attr("text-anchor", "middle")
        .attr("dy", ".35em")
        .attr("fill", "white")
        .attr("font-size", "12px")
        .attr("font-weight", "bold");

    simulation.on("tick", () => {
        link.attr("x1", d => d.source.x)
            .attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x)
            .attr("y2", d => d.target.y);
        node.attr("cx", d => d.x).attr("cy", d => d.y);
        label.attr("x", d => d.x).attr("y", d => d.y);
    });

    function drag(simulation) {
        function dragstarted(event) {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            event.subject.fx = event.subject.x;
            event.subject.fy = event.subject.y;
        }
        function dragged(event) {
            event.subject.fx = event.x;
            event.subject.fy = event.y;
        }
        function dragended(event) {
            if (!event.active) simulation.alphaTarget(0);
            event.subject.fx = null;
            event.subject.fy = null;
        }
        return d3.drag().on("start", dragstarted).on("drag", dragged).on("end", dragended);
    }
}

function populateSelects(nodes) {
    const srcSel = document.getElementById('srcSelect');
    const dstSel = document.getElementById('dstSelect');
    srcSel.innerHTML = nodes.map(n => `<option value="${n.id}">${n.name}</option>`).join('');
    dstSel.innerHTML = nodes.map(n => `<option value="${n.id}">${n.name}</option>`).join('');
}

function updateTable() {
    const div = document.getElementById('nodeTable');
    let html = '<table class="table table-dark table-sm"><thead><tr><th>Узел</th><th>TX (ipybyte)</th><th>RX (ipybyte)</th><th>Квота</th><th>Осталось</th></tr></thead><tbody>';
    nodesData.forEach(n => {
        html += `<tr>
            <td><strong>${n.name}</strong></td>
            <td>${n.tx_ipyb.toFixed(6)}</td>
            <td>${n.rx_ipyb.toFixed(6)}</td>
            <td>${n.quota_ipyb.toFixed(2)}</td>
            <td>${n.remaining_ipyb.toFixed(6)}</td>
        </tr>`;
    });
    html += '</tbody></table>';
    div.innerHTML = html;
}

function updateChart() {
    const ctx = document.getElementById('trafficChart').getContext('2d');
    const labels = nodesData.map(n => n.name);
    const txData = nodesData.map(n => n.tx_ipyb);
    const rxData = nodesData.map(n => n.rx_ipyb);
    if (trafficChart) trafficChart.destroy();
    trafficChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                { label: 'Передано (ipybyte)', data: txData, backgroundColor: '#ffaa44' },
                { label: 'Получено (ipybyte)', data: rxData, backgroundColor: '#44aaff' }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: { legend: { position: 'top', labels: { color: '#fff' } } },
            scales: { y: { ticks: { color: '#fff' } }, x: { ticks: { color: '#fff' } } }
        }
    });
}

document.getElementById('sendBtn').addEventListener('click', async () => {
    const src = document.getElementById('srcSelect').value;
    const dst = document.getElementById('dstSelect').value;
    const size = parseInt(document.getElementById('pktSize').value);
    const prio = document.getElementById('prioritySelect').value;
    if (isNaN(size) || size <= 0) {
        alert('Размер должен быть > 0');
        return;
    }
    const url = `/api/send?src=${encodeURIComponent(src)}&dst=${encodeURIComponent(dst)}&size=${size}&priority=${prio}`;
    try {
        const res = await fetch(url, { method: 'POST' });
        const data = await res.json();
        const resultDiv = document.getElementById('sendResult');
        if (res.ok) {
            resultDiv.innerHTML = `<span class="text-success">✅ ${data.message}</span>`;
        } else {
            resultDiv.innerHTML = `<span class="text-danger">❌ ${data.error}</span>`;
        }
        setTimeout(() => resultDiv.innerHTML = '', 3000);
    } catch (err) {
        console.error(err);
    }
});

loadTopology();
