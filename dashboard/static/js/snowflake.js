(() => {
    "use strict";

    const tableSelect = document.getElementById(
        "snowflake-source-table"
    );

    const publishButton = document.getElementById(
        "snowflake-publish-btn"
    );

    const output = document.getElementById(
        "snowflake-output"
    );

    if (!tableSelect || !publishButton || !output) return;

    const tables = [
        "orders",
        "order_items",
        "customers",
        "products",
        "payments",
        "reviews",
        "sellers",
        "geolocation",
        "category_translation"
    ];

    for (const table of tables) {
        const option = document.createElement("option");
        option.value = table;
        option.textContent = table.replaceAll("_", " ").toUpperCase();
        tableSelect.appendChild(option);
    }

    publishButton.disabled = false;

    publishButton.addEventListener("click", async () => {
        const table = tableSelect.value;
        const limit = Number(
            document.getElementById("snowflake-publish-limit").value
        );

        if (!table || !Number.isInteger(limit) ||
            limit < 1 || limit > 1000) {
            output.textContent =
                "Select a table and enter a limit between 1 and 1000.";
            return;
        }

        publishButton.disabled = true;
        output.textContent =
            `Publishing ${limit} records from ${table}...`;

        try {
            const response = await fetch("/api/snowflake/publish", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({table, limit})
            });

            const result = await response.json();

            if (!response.ok || !result.success) {
                throw new Error(
                    result.error || "Snowflake producer failed"
                );
            }

            output.textContent =
                result.output || "Producer completed successfully.";
            await refreshSnowflakeRuns();

        } catch (error) {
            output.textContent = error.message;
        } finally {
            publishButton.disabled = false;
        }
    });

    async function refreshSnowflakeStatus() {
    const status = document.getElementById("snowflake-status");
    const tables = document.getElementById("snowflake-tables");
    const tableList = document.getElementById("snowflake-table-list");
    const pipeline = document.getElementById("snowflake-pipeline-info");

    if (!status || !tables || !tableList || !pipeline) return;

    try {
        const response = await fetch("/api/snowflake/status", {
            cache: "no-store"
        });

        const data = await response.json();

        if (!response.ok || !data.connected) {
            throw new Error(data.error || "Connection unavailable");
        }

        status.textContent = "Connected";
        status.style.color = "#15803D";

        tables.textContent = data.table_count;

        tableList.replaceChildren();

        for (const table of data.tables) {
            const item = document.createElement("div");
            item.textContent = table;
            item.style.padding = "10px 0";
            item.style.borderBottom = "1px solid #E2E8F0";
            tableList.appendChild(item);
        }

        pipeline.textContent =
            `Connected to ${data.database}.${data.schema} · ` +
            `${data.table_count} Olist tables available`;

    } catch (error) {
        status.textContent = "Disconnected";
        status.style.color = "#DC2626";
        tables.textContent = "--";
        tableList.textContent = "Unable to load Snowflake tables.";
        pipeline.textContent = error.message;
    }
}

refreshSnowflakeStatus();


async function refreshSnowflakeRuns() {
    const records = document.getElementById(
    "snowflake-published"
);

    const lastRun = document.getElementById(
        "snowflake-last-run"
    );

    if (!records || !lastRun) return;

    try {
        const response = await fetch("/api/snowflake/runs", {
            cache: "no-store"
        });

        if (!response.ok) throw new Error("Unable to load run history");

        const data = await response.json();

        records.textContent =
            Number(data.total_published).toLocaleString();

        lastRun.textContent = data.last_run
            ? new Date(data.last_run).toLocaleString()
            : "No recorded runs";

    } catch (error) {
        records.textContent = "--";
        lastRun.textContent = "--";
        console.error("Snowflake run history:", error);
    }
}

refreshSnowflakeRuns();
})();