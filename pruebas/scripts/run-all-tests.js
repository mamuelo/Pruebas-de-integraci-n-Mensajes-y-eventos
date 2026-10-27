/**
 * Ejecutor Maestro de Pruebas de Integración
 * Ejecuta ambas suites (RabbitMQ y Kafka), consolida los 20 reportes individuales
 * y genera los artefactos de evidencia y la matriz resumen para el informe.
 */

const fs = require('fs');
const path = require('path');
const { runRabbitMQTests } = require('./test-rabbitmq');
const { runKafkaTests } = require('./test-kafka');

async function main() {
  console.log('################################################################');
  console.log('#   TALLER: PRUEBAS DE INTEGRACIÓN EN ARQUITECTURAS REACTIVAS  #');
  console.log('#   EJECUTOR AUTOMATIZADO DE SUITES RABBITMQ Y APACHE KAFKA    #');
  console.log('################################################################\n');

  const startTime = Date.now();
  const resultadosDir = path.join(__dirname, '..', 'resultados');
  if (!fs.existsSync(resultadosDir)) {
    fs.mkdirSync(resultadosDir, { recursive: true });
  }

  // 1. Ejecutar pruebas de RabbitMQ
  console.log('>>> [1/2] Ejecutando suite de pruebas para Arquitectura 1 (RabbitMQ)...');
  const rabbitmqReports = await runRabbitMQTests();

  console.log('\n>>> [2/2] Ejecutando suite de pruebas para Arquitectura 2 (Apache Kafka)...');
  const kafkaReports = await runKafkaTests();

  const totalDuration = Date.now() - startTime;
  const allReports = [...rabbitmqReports, ...kafkaReports];

  // Consolidar resultados globales
  const consolidated = {
    metadata: {
      institucion: 'ITM - Institución Universitaria',
      asignatura: 'Aseguramiento de la Calidad (ACS)',
      taller: 'Pruebas de Integración en Arquitecturas Basadas en Mensajes y Eventos',
      fechaEjecucion: new Date().toISOString(),
      duracionTotalMs: totalDuration,
      totalCasos: allReports.length,
      cumplidos: allReports.filter((r) => r.estado === 'Cumple').length,
      noCumplidos: allReports.filter((r) => r.estado === 'No cumple').length,
      pendientes: allReports.filter((r) => r.estado === 'Pendiente').length
    },
    casos: allReports
  };

  const jsonSummaryPath = path.join(resultadosDir, 'resultados-ejecucion.json');
  fs.writeFileSync(jsonSummaryPath, JSON.stringify(consolidated, null, 2), 'utf-8');

  // Generar matriz resumen en Markdown
  let markdown = `# Matriz Resumen de Ejecución de Pruebas de Integración\n\n`;
  markdown += `**Fecha de Ejecución:** ${consolidated.metadata.fechaEjecucion}  \n`;
  markdown += `**Total Casos Evaluados:** ${consolidated.metadata.totalCasos} | **Cumple:** ${consolidated.metadata.cumplidos} | **No Cumple:** ${consolidated.metadata.noCumplidos} | **Pendientes:** ${consolidated.metadata.pendientes}\n\n`;

  markdown += `| ID | Caso de Prueba | Arquitectura | Tipo | Estado | Resultado Obtenido |\n`;
  markdown += `|---|---|---|---|---|---|\n`;

  for (const r of allReports) {
    const tipo = ['CP-01', 'CP-02', 'CP-03', 'CP-04', 'CP-05', 'CP-06'].includes(r.id)
      ? 'Funcional'
      : 'Reactiva';
    const statusBadge =
      r.estado === 'Cumple'
        ? '✅ Cumple'
        : r.estado === 'No cumple'
        ? '❌ No cumple'
        : '⏳ Pendiente';
    markdown += `| **${r.id}** | ${r.objetivo.substring(0, 45)}... | ${r.arquitectura} | ${tipo} | ${statusBadge} | ${r.resultadoObtenido.substring(0, 70)}... |\n`;
  }

  const mdSummaryPath = path.join(resultadosDir, 'matriz-evidencias.md');
  fs.writeFileSync(mdSummaryPath, markdown, 'utf-8');

  console.log('\n================================================================');
  console.log('   RESUMEN GENERAL DE EJECUCIÓN                                 ');
  console.log('================================================================');
  console.log(`Total casos: ${consolidated.metadata.totalCasos}`);
  console.log(`Cumple:      ${consolidated.metadata.cumplidos}`);
  console.log(`No cumple:   ${consolidated.metadata.noCumplidos}`);
  console.log(`Pendientes:  ${consolidated.metadata.pendientes}`);
  console.log(`Duración:    ${(totalDuration / 1000).toFixed(2)}s`);
  console.log(`Artefactos:  ${jsonSummaryPath}`);
  console.log(`             ${mdSummaryPath}`);
  console.log('================================================================\n');
}

if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main };
