/**
 * CÉREBRO DE ATENDIMENTO v2.0 - SISTEMA COMPLETO E SIMPLIFICADO
 * Sistema híbrido: Performance + Simplicidade + Funcionalidades essenciais
 * 
 * ✅ Baseado no projeto v1 que funcionava
 * ✅ Melhorias do v3.2 que fazem sentido
 * ✅ Sistema de contatos automático
 * ✅ Horário Brasília/Bahia correto
 * ✅ Dashboard simples e funcional
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const moment = require('moment-timezone');
const axios = require('axios');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Middlewares
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// Configurações
const CONFIG = {
    PIX_TIMEOUT: parseInt(process.env.PIX_TIMEOUT) || 420000, // 7 minutos
    N8N_WEBHOOK_URL: process.env.N8N_WEBHOOK_URL,
    EVOLUTION_API_URL: process.env.EVOLUTION_API_URL,
    MAX_RETRY_ATTEMPTS: parseInt(process.env.MAX_RETRY_ATTEMPTS) || 3
};

// Conexão PostgreSQL
let database;
async function connectDatabase() {
    try {
        console.log('🔌 Conectando ao PostgreSQL...');
        
        const config = {
            connectionString: process.env.DATABASE_URL,
            ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
            max: 10,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 5000,
        };

        database = new Pool(config);
        
        // Testar conexão
        const client = await database.connect();
        const result = await client.query('SELECT NOW() as current_time');
        client.release();
        
        console.log('✅ PostgreSQL conectado');
        console.log(`📅 Hora do servidor: ${result.rows[0].current_time}`);
        
    } catch (error) {
        console.error(`❌ Erro PostgreSQL: ${error.message}`);
        process.exit(1);
    }
}

// Mapeamento de produtos
const PRODUCT_MAPPING = {
    'PPLQQM9AP': 'FAB',
    'PPLQQMAGU': 'FAB', 
    'PPLQQMADF': 'FAB',
    'PPLQQN0FT': 'NAT',
    'PPLQQMSFH': 'CS',
    'PPLQQMSFI': 'CS'
};

// Instâncias Evolution API
const INSTANCES = [
    { name: 'GABY01', id: '1CEBB8703497-4F31-B33F-335A4233D2FE', active: true },
    { name: 'GABY02', id: '939E26DEA1FA-40D4-83CE-2BF0B3F795DC', active: true },
    { name: 'GABY03', id: 'F819629B5E33-435B-93BB-091B4C104C12', active: true },
    { name: 'GABY04', id: 'D555A7CBC0B3-425B-8E20-975232BE75F6', active: true },
    { name: 'GABY05', id: 'D97A63B8B05B-430E-98C1-61977A51EC0B', active: true },
    { name: 'GABY06', id: '6FC2C4C703BA-4A8A-9B3B-21536AE51323', active: true },
    { name: 'GABY07', id: '14F637AB35CD-448D-BF66-5673950FBA10', active: true },
    { name: 'GABY08', id: '82E0CE5B1A51-4B7B-BBEF-77D22320B482', active: true },
    { name: 'GABY09', id: 'B5783C928EF4-4DB0-ABBA-AF6913116E7B', active: true }
];

// Armazenamento híbrido: Memória (rápido) + Banco (persistente)
let conversations = new Map(); // Cache em memória
let pendingTimeouts = new Map(); // Timeouts ativos
let instanceCounter = 0;
let systemStats = {
    totalEvents: 0,
    successfulEvents: 0,
    failedEvents: 0,
    contactsSaved: 0,
    startTime: new Date()
};

/**
 * UTILITÁRIOS
 */

// Horário Brasília/Bahia
function getBrazilTime(format = 'DD/MM/YYYY HH:mm:ss') {
    return moment().tz('America/Bahia').format(format);
}

// Normalizar telefone (versão corrigida)
function normalizePhoneNumber(phone) {
    if (!phone) return phone;
    
    console.log(`📱 Normalizando: ${phone}`);
    
    let cleaned = String(phone).trim().replace(/\D/g, '');
    
    // Formato brasileiro padrão: 5511999999999 (13 dígitos)
    if (cleaned.length === 14 && cleaned.startsWith('55')) {
        const areaCode = cleaned.substring(2, 4);
        const rest = cleaned.substring(4);
        
        // Remover 9 extra se necessário
        if (rest.length === 10 && rest[0] === '9' && rest[1] !== '9') {
            cleaned = '55' + areaCode + rest.substring(1);
        }
    } else if (cleaned.length === 11) {
        cleaned = '55' + cleaned;
    }
    
    console.log(`✅ Normalizado: ${phone} → ${cleaned}`);
    return cleaned;
}

// Extrair primeiro nome
function getFirstName(fullName) {
    return fullName ? fullName.split(' ')[0].trim() : 'Cliente';
}

// Obter produto pelo código
function getProductByPlanCode(planCode) {
    return PRODUCT_MAPPING[planCode] || 'UNKNOWN';
}

// Formatar telefone Perfect Pay
function formatPhoneFromPerfectPay(extension, areaCode, number) {
    const full = (extension || '55') + (areaCode || '') + (number || '');
    return normalizePhoneNumber(full);
}

/**
 * SISTEMA DE INSTÂNCIAS (STICKY SESSION)
 */
async function getInstanceForClient(clientPhone) {
    try {
        const normalizedPhone = normalizePhoneNumber(clientPhone);
        console.log(`🔍 Buscando instância para: ${normalizedPhone}`);
        
        // Verificar cache primeiro
        for (const [phone, conv] of conversations) {
            if (normalizePhoneNumber(phone) === normalizedPhone) {
                console.log(`👤 Cliente já tem instância: ${conv.instance}`);
                return conv.instance;
            }
        }
        
        // Verificar banco
        try {
            const result = await database.query(
                'SELECT instance_name FROM conversations WHERE phone = $1 ORDER BY created_at DESC LIMIT 1',
                [normalizedPhone]
            );
            
            if (result.rows.length > 0) {
                const instance = result.rows[0].instance_name;
                console.log(`💾 Instância do banco: ${instance}`);
                return instance;
            }
        } catch (dbError) {
            console.warn(`⚠️ Erro no banco, usando balanceamento: ${dbError.message}`);
        }
        
        // Balanceamento round-robin simples
        const instance = INSTANCES[instanceCounter % INSTANCES.length];
        instanceCounter++;
        
        console.log(`⚖️ Nova instância atribuída: ${instance.name}`);
        return instance.name;
        
    } catch (error) {
        console.error(`❌ Erro ao obter instância: ${error.message}`);
        return 'GABY01'; // Fallback
    }
}

/**
 * SISTEMA DE CONTATOS AUTOMÁTICO
 */

// Verificar se deve salvar contato
function shouldSaveContact(messageContent, conversationData) {
    try {
        // Palavras que indicam desinteresse
        const stopKeywords = [
            'pare', 'parar', 'sair', 'cancelar', 'remover', 
            'não quero', 'não tenho interesse', 'remove'
        ];
        
        const messageClean = messageContent.toLowerCase().trim();
        const wantsToStop = stopKeywords.some(keyword => 
            messageClean.includes(keyword)
        );
        
        if (wantsToStop) {
            console.log(`🚫 Cliente não quer contato: "${messageContent.substring(0, 50)}..."`);
            return false;
        }
        
        // Mensagem muito curta
        if (messageContent.trim().length < 2) {
            console.log(`📵 Mensagem muito curta: "${messageContent}"`);
            return false;
        }
        
        return true;
        
    } catch (error) {
        console.error(`❌ Erro ao verificar contato: ${error.message}`);
        return true; // Em caso de erro, salvar
    }
}

// Salvar contato automaticamente
async function saveContactAutomatically(phone, instanceName, conversationData) {
    try {
        const today = getBrazilTime('DD/MM');
        
        console.log(`📇 Salvando contato: ${phone} | ${today} | ${instanceName}`);
        
        // Verificar se já existe
        const existing = await database.query(
            'SELECT id FROM contacts WHERE phone = $1 AND name = $2',
            [phone, today]
        );
        
        if (existing.rows.length > 0) {
            console.log(`📇 Contato já existe hoje: ${phone}`);
            return { success: true, action: 'exists' };
        }
        
        // Salvar novo
        const result = await database.query(`
            INSERT INTO contacts (phone, name, instance, product, conversation_id) 
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        `, [
            phone, 
            today, 
            instanceName, 
            conversationData.product || 'UNKNOWN',
            conversationData.id || null
        ]);
        
        const contactId = result.rows[0].id;
        systemStats.contactsSaved++;
        
        console.log(`✅ Contato salvo: ID ${contactId} | ${today} | ${phone} | ${instanceName}`);
        
        return { 
            success: true, 
            action: 'saved', 
            contact_id: contactId,
            date: today
        };
        
    } catch (error) {
        if (error.message.includes('unique_phone_date')) {
            console.log(`📇 Duplicata ignorada: ${phone}`);
            return { success: true, action: 'duplicate' };
        }
        
        console.error(`❌ Erro ao salvar contato: ${phone} | ${error.message}`);
        return { success: false, error: error.message };
    }
}

/**
 * SISTEMA DE ENVIO PARA N8N
 */
async function sendToN8N(eventData, eventType, attempt = 1) {
    const maxAttempts = CONFIG.MAX_RETRY_ATTEMPTS;
    
    try {
        console.log(`📤 Enviando para N8N (${attempt}/${maxAttempts}): ${eventType}`);
        console.log(`🎯 URL: ${CONFIG.N8N_WEBHOOK_URL}`);
        
        const response = await axios.post(CONFIG.N8N_WEBHOOK_URL, eventData, {
            headers: {
                'Content-Type': 'application/json',
                'User-Agent': 'Cerebro-v2.0/1.0'
            },
            timeout: 15000
        });
        
        console.log(`✅ N8N sucesso: ${eventType} | Status: ${response.status}`);
        systemStats.successfulEvents++;
        return true;
        
    } catch (error) {
        const errorMsg = error.response ? 
            `HTTP ${error.response.status}: ${error.response.statusText}` : 
            error.message;
            
        console.error(`❌ N8N erro (${attempt}/${maxAttempts}): ${errorMsg}`);
        
        if (attempt < maxAttempts) {
            const delay = attempt * 2000;
            console.log(`🔄 Retry em ${delay/1000}s...`);
            
            return new Promise((resolve) => {
                setTimeout(async () => {
                    const result = await sendToN8N(eventData, eventType, attempt + 1);
                    resolve(result);
                }, delay);
            });
        }
        
        systemStats.failedEvents++;
        return false;
    }
}

/**
 * WEBHOOK PERFECT PAY
 */
app.post('/webhook/perfect', async (req, res) => {
    try {
        const data = req.body;
        const orderCode = data.code;
        const status = data.sale_status_enum_key;
        const planCode = data.plan?.code;
        const product = getProductByPlanCode(planCode);
        
        const fullName = data.customer?.full_name || 'Cliente';
        const firstName = getFirstName(fullName);
        const phoneNumber = formatPhoneFromPerfectPay(
            data.customer?.phone_extension,
            data.customer?.phone_area_code,
            data.customer?.phone_number
        );
        const amount = parseFloat(data.sale_amount) || 0;
        const pixUrl = data.billet_url || '';
        
        console.log(`📥 PERFECT: ${orderCode} | ${status} | ${product} | ${phoneNumber}`);
        systemStats.totalEvents++;
        
        if (status === 'approved') {
            await handleApprovedSale(orderCode, phoneNumber, firstName, fullName, product, amount, data);
        } else if (status === 'pending') {
            await handlePendingPix(orderCode, phoneNumber, firstName, fullName, product, amount, pixUrl, planCode, data);
        }
        
        res.status(200).json({ 
            success: true, 
            message: 'Perfect Pay processado',
            order_code: orderCode,
            product: product,
            phone_normalized: phoneNumber
        });
        
    } catch (error) {
        console.error(`❌ Erro Perfect: ${error.message}`);
        systemStats.failedEvents++;
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * PROCESSAR VENDA APROVADA
 */
async function handleApprovedSale(orderCode, phoneNumber, firstName, fullName, product, amount, originalData) {
    try {
        console.log(`💰 VENDA APROVADA: ${orderCode} | ${product} | ${firstName}`);
        
        const instanceName = await getInstanceForClient(phoneNumber);
        
        // Cancelar timeout se existir
        if (pendingTimeouts.has(orderCode)) {
            clearTimeout(pendingTimeouts.get(orderCode));
            pendingTimeouts.delete(orderCode);
            console.log(`🗑️ Timeout PIX cancelado: ${orderCode}`);
        }
        
        // Salvar em memória
        const conversation = {
            phone: phoneNumber,
            orderCode: orderCode,
            product: product,
            status: 'approved',
            currentStep: 0,
            instance: instanceName,
            amount: amount,
            clientName: fullName,
            createdAt: new Date(),
            lastActivity: new Date(),
            responseCount: 0,
            pixUrl: '',
            id: Date.now() // ID temporário
        };
        
        conversations.set(phoneNumber, conversation);
        
        // Salvar no banco (async)
        try {
            await database.query(`
                INSERT INTO conversations 
                (phone, order_code, product, status, instance_name, amount, client_name, created_at, updated_at)
                VALUES ($1, $2, $3, 'approved', $4, $5, $6, NOW(), NOW())
                ON CONFLICT (order_code) 
                DO UPDATE SET 
                    status = 'approved',
                    instance_name = $4,
                    amount = $5,
                    client_name = $6,
                    updated_at = NOW()
            `, [phoneNumber, orderCode, product, instanceName, amount, fullName]);
        } catch (dbError) {
            console.warn(`⚠️ Erro no banco (aprovada): ${dbError.message}`);
        }
        
        // Enviar para N8N
        const eventData = {
            event_type: 'venda_aprovada',
            produto: product,
            instancia: instanceName,
            evento_origem: 'aprovada',
            cliente: {
                nome: firstName,
                telefone: phoneNumber,
                nome_completo: fullName
            },
            pedido: {
                codigo: orderCode,
                valor: amount
            },
            timestamp: new Date().toISOString(),
            brazil_time: getBrazilTime(),
            conversation_id: conversation.id
        };
        
        await sendToN8N(eventData, 'venda_aprovada');
        
        console.log(`✅ Venda aprovada processada: ${orderCode}`);
        
    } catch (error) {
        console.error(`❌ Erro venda aprovada: ${error.message}`);
    }
}

/**
 * PROCESSAR PIX PENDENTE
 */
async function handlePendingPix(orderCode, phoneNumber, firstName, fullName, product, amount, pixUrl, planCode, originalData) {
    try {
        console.log(`⏰ PIX GERADO: ${orderCode} | ${product} | ${firstName}`);
        
        const instanceName = await getInstanceForClient(phoneNumber);
        
        // Cancelar timeout anterior se existir
        if (pendingTimeouts.has(orderCode)) {
            clearTimeout(pendingTimeouts.get(orderCode));
            pendingTimeouts.delete(orderCode);
        }
        
        // Salvar em memória
        const conversation = {
            phone: phoneNumber,
            orderCode: orderCode,
            product: product,
            status: 'pix_pending',
            currentStep: 0,
            instance: instanceName,
            amount: amount,
            clientName: fullName,
            createdAt: new Date(),
            lastActivity: new Date(),
            responseCount: 0,
            pixUrl: pixUrl,
            id: Date.now() // ID temporário
        };
        
        conversations.set(phoneNumber, conversation);
        
        // Salvar no banco (async)
        try {
            await database.query(`
                INSERT INTO conversations 
                (phone, order_code, product, status, instance_name, amount, pix_url, client_name, created_at, updated_at)
                VALUES ($1, $2, $3, 'pix_pending', $4, $5, $6, $7, NOW(), NOW())
                ON CONFLICT (order_code) 
                DO UPDATE SET 
                    status = 'pix_pending',
                    instance_name = $4,
                    amount = $5,
                    pix_url = $6,
                    client_name = $7,
                    updated_at = NOW()
            `, [phoneNumber, orderCode, product, instanceName, amount, pixUrl, fullName]);
        } catch (dbError) {
            console.warn(`⚠️ Erro no banco (PIX): ${dbError.message}`);
        }
        
        // Criar timeout de 7 minutos
        const timeout = setTimeout(async () => {
            console.log(`⏰ TIMEOUT PIX: ${orderCode}`);
            pendingTimeouts.delete(orderCode);
            await handlePixTimeout(orderCode, conversation);
        }, CONFIG.PIX_TIMEOUT);
        
        pendingTimeouts.set(orderCode, timeout);
        
        console.log(`✅ PIX pendente registrado: ${orderCode} | ${Math.round(CONFIG.PIX_TIMEOUT/60000)} min`);
        
    } catch (error) {
        console.error(`❌ Erro PIX pendente: ${error.message}`);
    }
}

/**
 * PROCESSAR TIMEOUT PIX
 */
async function handlePixTimeout(orderCode, conversation) {
    try {
        console.log(`⏰ Processando timeout PIX: ${orderCode}`);
        
        // Verificar se ainda está pendente
        const currentConv = conversations.get(conversation.phone);
        if (!currentConv || currentConv.status !== 'pix_pending') {
            console.log(`ℹ️ PIX ${orderCode} não está mais pendente`);
            return;
        }
        
        // Atualizar status
        currentConv.status = 'timeout';
        currentConv.lastActivity = new Date();
        conversations.set(conversation.phone, currentConv);
        
        // Atualizar banco (async)
        try {
            await database.query(
                'UPDATE conversations SET status = $1, updated_at = NOW() WHERE order_code = $2',
                ['timeout', orderCode]
            );
        } catch (dbError) {
            console.warn(`⚠️ Erro no banco (timeout): ${dbError.message}`);
        }
        
        // Enviar para N8N
        const firstName = getFirstName(conversation.clientName);
        const eventData = {
            event_type: 'pix_timeout',
            produto: conversation.product,
            instancia: conversation.instance,
            evento_origem: 'pix',
            cliente: {
                nome: firstName,
                telefone: conversation.phone,
                nome_completo: conversation.clientName
            },
            pedido: {
                codigo: orderCode,
                valor: conversation.amount,
                pix_url: conversation.pixUrl
            },
            timeout_minutos: 7,
            timestamp: new Date().toISOString(),
            brazil_time: getBrazilTime(),
            conversation_id: conversation.id
        };
        
        await sendToN8N(eventData, 'pix_timeout');
        
        console.log(`✅ Timeout PIX enviado: ${orderCode}`);
        
    } catch (error) {
        console.error(`❌ Erro timeout PIX: ${error.message}`);
    }
}

/**
 * WEBHOOK EVOLUTION API
 */
app.post('/webhook/evolution', async (req, res) => {
    try {
        const data = req.body;
        const messageData = data.data;
        
        if (!messageData || !messageData.key) {
            return res.status(200).json({ success: true, message: 'Estrutura inválida' });
        }
        
        const remoteJid = messageData.key.remoteJid;
        const fromMe = messageData.key.fromMe;
        const messageContent = messageData.message?.conversation || 
                              messageData.message?.extendedTextMessage?.text || '';
        const instanceName = data.instance;
        
        const clientNumber = normalizePhoneNumber(remoteJid.replace('@s.whatsapp.net', ''));
        
        console.log(`📱 Evolution: ${fromMe ? 'Sistema' : 'Cliente'} | ${clientNumber} | ${instanceName}`);
        systemStats.totalEvents++;
        
        if (fromMe) {
            await handleSystemMessage(clientNumber, messageContent, instanceName);
        } else {
            await handleClientResponse(clientNumber, messageContent, instanceName, messageData);
        }
        
        res.status(200).json({ 
            success: true, 
            message: 'Evolution processado',
            client_number: clientNumber,
            from_me: fromMe
        });
        
    } catch (error) {
        console.error(`❌ Erro Evolution: ${error.message}`);
        systemStats.failedEvents++;
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * PROCESSAR MENSAGEM DO SISTEMA
 */
async function handleSystemMessage(clientNumber, messageContent, instanceName) {
    try {
        console.log(`📤 Mensagem do sistema: ${clientNumber}`);
        
        // Buscar conversa
        const conversation = conversations.get(clientNumber);
        if (conversation) {
            conversation.lastActivity = new Date();
            conversations.set(clientNumber, conversation);
        }
        
    } catch (error) {
        console.error(`❌ Erro mensagem sistema: ${error.message}`);
    }
}

/**
 * PROCESSAR RESPOSTA DO CLIENTE
 */
async function handleClientResponse(clientNumber, messageContent, instanceName, messageData) {
    try {
        console.log(`📥 RESPOSTA CLIENTE: ${clientNumber} | "${messageContent.substring(0, 50)}..."`);
        
        // Buscar conversa ativa
        const conversation = conversations.get(clientNumber);
        
        if (!conversation) {
            console.log(`⚠️ Cliente ${clientNumber} não encontrado nas conversas ativas`);
            return;
        }
        
        // Verificar se PIX foi pago durante o fluxo
        if (conversation.status === 'pix_pending') {
            const isPaid = await checkPaymentStatus(conversation.orderCode);
            
            if (isPaid) {
                console.log(`🎉 PIX pago durante fluxo - enviando convertido`);
                
                // Cancelar timeout
                if (pendingTimeouts.has(conversation.orderCode)) {
                    clearTimeout(pendingTimeouts.get(conversation.orderCode));
                    pendingTimeouts.delete(conversation.orderCode);
                }
                
                // Atualizar status
                conversation.status = 'convertido';
                conversation.lastActivity = new Date();
                conversations.set(clientNumber, conversation);
                
                // Salvar contato se primeira resposta
                if (conversation.responseCount === 0) {
                    if (shouldSaveContact(messageContent, conversation)) {
                        await saveContactAutomatically(clientNumber, conversation.instance, conversation);
                    }
                }
                
                // Enviar evento convertido
                await sendConversionEvent(conversation, messageContent);
                return;
            }
        }
        
        // Determinar próximo passo baseado em responses_count
        const nextStep = conversation.responseCount + 1;
        
        if (nextStep > 3) {
            console.log(`✅ Funil completo - ${clientNumber} já recebeu 3 respostas`);
            return;
        }
        
        console.log(`📋 Cliente ${clientNumber} - enviando resposta_0${nextStep}`);
        
        // SALVAR CONTATO AUTOMATICAMENTE (apenas na primeira resposta)
        if (nextStep === 1) {
            if (shouldSaveContact(messageContent, conversation)) {
                await saveContactAutomatically(clientNumber, conversation.instance, conversation);
            }
        }
        
        // Atualizar conversa
        conversation.responseCount = nextStep;
        conversation.lastActivity = new Date();
        conversations.set(clientNumber, conversation);
        
        // Atualizar banco (async)
        try {
            await database.query(
                'UPDATE conversations SET responses_count = $1, last_response_at = NOW(), updated_at = NOW() WHERE order_code = $2',
                [nextStep, conversation.orderCode]
            );
        } catch (dbError) {
            console.warn(`⚠️ Erro no banco (resposta): ${dbError.message}`);
        }
        
        // Preparar dados para N8N
        const firstName = getFirstName(conversation.clientName);
        const eventData = {
            event_type: `resposta_0${nextStep}`,
            produto: conversation.product,
            instancia: conversation.instance,
            evento_origem: conversation.status === 'approved' ? 'aprovada' : 'pix',
            cliente: {
                telefone: conversation.phone,
                nome: firstName,
                nome_completo: conversation.clientName
            },
            resposta: {
                numero: nextStep,
                conteudo: messageContent,
                timestamp: new Date().toISOString(),
                brazil_time: getBrazilTime()
            },
            pedido: {
                codigo: conversation.orderCode,
                valor: conversation.amount,
                pix_url: conversation.pixUrl || ''
            },
            timestamp: new Date().toISOString(),
            brazil_time: getBrazilTime(),
            conversation_id: conversation.id
        };
        
        // Enviar para N8N
        const success = await sendToN8N(eventData, `resposta_0${nextStep}`);
        
        if (success) {
            console.log(`✅ Resposta_0${nextStep} enviada com sucesso`);
            
            // Se foi a terceira resposta, marcar como completo
            if (nextStep === 3) {
                conversation.status = 'completed';
                conversations.set(clientNumber, conversation);
                
                try {
                    await database.query(
                        'UPDATE conversations SET status = $1, updated_at = NOW() WHERE order_code = $2',
                        ['completed', conversation.orderCode]
                    );
                } catch (dbError) {
                    console.warn(`⚠️ Erro no banco (completo): ${dbError.message}`);
                }
                
                console.log(`🎯 Funil completo: ${conversation.orderCode}`);
            }
        } else {
            console.error(`❌ Falha ao enviar resposta_0${nextStep}`);
        }
        
    } catch (error) {
        console.error(`❌ Erro resposta cliente: ${error.message}`);
    }
}

/**
 * VERIFICAR STATUS DE PAGAMENTO
 */
async function checkPaymentStatus(orderCode) {
    try {
        // Verificar na memória primeiro
        for (const [phone, conv] of conversations) {
            if (conv.orderCode === orderCode) {
                return conv.status === 'approved' || conv.status === 'completed';
            }
        }
        
        // Verificar no banco
        try {
            const result = await database.query(
                'SELECT status FROM conversations WHERE order_code = $1 ORDER BY updated_at DESC LIMIT 1',
                [orderCode]
            );
            
            if (result.rows.length > 0) {
                const status = result.rows[0].status;
                return status === 'approved' || status === 'completed';
            }
        } catch (dbError) {
            console.warn(`⚠️ Erro verificar pagamento: ${dbError.message}`);
        }
        
        return false;
        
    } catch (error) {
        console.error(`❌ Erro verificar pagamento: ${error.message}`);
        return false;
    }
}

/**
 * ENVIAR EVENTO DE CONVERSÃO
 */
async function sendConversionEvent(conversation, messageContent) {
    try {
        const firstName = getFirstName(conversation.clientName);
        
        console.log(`🎯 Enviando evento convertido: ${conversation.orderCode}`);
        
        const eventData = {
            event_type: 'convertido',
            produto: conversation.product,
            instancia: conversation.instance,
            evento_origem: 'pix_convertido',
            cliente: {
                telefone: conversation.phone,
                nome: firstName,
                nome_completo: conversation.clientName
            },
            conversao: {
                resposta_numero: conversation.responseCount + 1,
                conteudo_resposta: messageContent,
                valor_original: conversation.amount,
                timestamp: new Date().toISOString(),
                brazil_time: getBrazilTime()
            },
            pedido: {
                codigo: conversation.orderCode,
                valor: conversation.amount,
                pix_url: conversation.pixUrl || ''
            },
            timestamp: new Date().toISOString(),
            brazil_time: getBrazilTime(),
            conversation_id: conversation.id
        };
        
        const success = await sendToN8N(eventData, 'convertido');
        
        if (success) {
            console.log(`✅ Evento convertido enviado: ${conversation.orderCode}`);
        }
        
        return success;
        
    } catch (error) {
        console.error(`❌ Erro evento conversão: ${error.message}`);
        return false;
    }
}

/**
 * WEBHOOK N8N CONFIRM
 */
app.post('/webhook/n8n-confirm', async (req, res) => {
    try {
        const { tipo_mensagem, telefone, instancia } = req.body;
        
        const phoneNormalized = normalizePhoneNumber(telefone);
        
        console.log(`✅ N8N confirmou: ${tipo_mensagem} para ${phoneNormalized} via ${instancia}`);
        
        // Buscar conversa
        const conversation = conversations.get(phoneNormalized);
        
        if (!conversation) {
            console.warn(`⚠️ Conversa não encontrada para confirmação: ${phoneNormalized}`);
            return res.json({ 
                success: false, 
                message: 'Conversa não encontrada'
            });
        }
        
        // Atualizar atividade
        conversation.lastActivity = new Date();
        conversations.set(phoneNormalized, conversation);
        
        console.log(`📝 Confirmação N8N registrada: ${conversation.orderCode}`);
        
        res.json({ 
            success: true,
            message: `${tipo_mensagem} confirmada`,
            pedido: conversation.orderCode,
            cliente: conversation.clientName,
            respostas_atuais: conversation.responseCount,
            status_conversa: conversation.status
        });
        
    } catch (error) {
        console.error(`❌ Erro N8N confirm: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * ENDPOINTS PARA N8N
 */

// Verificar pagamento
app.get('/check-payment/:orderId', async (req, res) => {
    try {
        const { orderId } = req.params;
        
        console.log(`💳 Check payment: ${orderId}`);
        
        const isPaid = await checkPaymentStatus(orderId);
        
        res.json({ 
            status: isPaid ? 'paid' : 'pending',
            order_id: orderId
        });
        
    } catch (error) {
        console.error(`❌ Erro check payment: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Marcar como completo
app.post('/webhook/complete/:orderId', async (req, res) => {
    try {
        const { orderId } = req.params;
        
        console.log(`✅ Marcando completo: ${orderId}`);
        
        // Buscar e atualizar conversa
        for (const [phone, conv] of conversations) {
            if (conv.orderCode === orderId) {
                conv.status = 'completed';
                conv.lastActivity = new Date();
                conversations.set(phone, conv);
                break;
            }
        }
        
        // Atualizar banco (async)
        try {
            await database.query(
                'UPDATE conversations SET status = $1, updated_at = NOW() WHERE order_code = $2',
                ['completed', orderId]
            );
        } catch (dbError) {
            console.warn(`⚠️ Erro no banco (complete): ${dbError.message}`);
        }
        
        res.json({ 
            success: true, 
            message: 'Fluxo marcado como completo',
            order_id: orderId
        });
        
    } catch (error) {
        console.error(`❌ Erro marcar completo: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * ENDPOINTS DE CONTATOS
 */

// Estatísticas de contatos
app.get('/contacts/stats', async (req, res) => {
    try {
        const stats = await database.query(`
            SELECT 
                instance,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE saved_at >= NOW() - INTERVAL '24 hours') as today,
                COUNT(*) FILTER (WHERE saved_at >= NOW() - INTERVAL '7 days') as this_week,
                MIN(saved_at) as first_contact,
                MAX(saved_at) as last_contact
            FROM contacts 
            GROUP BY instance 
            ORDER BY total DESC
        `);
        
        const totalContacts = await database.query('SELECT COUNT(*) as total FROM contacts');
        
        res.json({
            total_contacts: parseInt(totalContacts.rows[0].total),
            by_instance: stats.rows.map(row => ({
                instance: row.instance,
                total: parseInt(row.total),
                today: parseInt(row.today),
                this_week: parseInt(row.this_week),
                first_contact: row.first_contact,
                last_contact: row.last_contact
            }))
        });
        
    } catch (error) {
        console.error(`❌ Erro stats contatos: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Exportar contatos por instância
app.get('/contacts/export/:instance', async (req, res) => {
    try {
        const { instance } = req.params;
        
        console.log(`📥 Exportando contatos: ${instance}`);
        
        const contacts = await database.query(`
            SELECT phone, name, saved_at, product
            FROM contacts 
            WHERE instance = $1 
            ORDER BY saved_at DESC
        `, [instance.toUpperCase()]);
        
        if (contacts.rows.length === 0) {
            return res.status(404).json({ 
                success: false, 
                message: `Nenhum contato encontrado para ${instance}` 
            });
        }
        
        // CSV formato Google Contacts
        let csv = 'Name,Phone 1 - Value,Notes\n';
        
        contacts.rows.forEach(contact => {
            const date = getBrazilTime('DD/MM/YYYY HH:mm');
            const notes = `Instância: ${instance} | Produto: ${contact.product} | Salvo: ${date}`;
            csv += `"${contact.name}","${contact.phone}","${notes}"\n`;
        });
        
        const filename = `contatos_${instance.toLowerCase()}_${getBrazilTime('YYYY-MM-DD')}.csv`;
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.send(csv);
        
        console.log(`✅ ${contacts.rows.length} contatos exportados: ${instance}`);
        
    } catch (error) {
        console.error(`❌ Erro exportar contatos: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Exportar todos os contatos
app.get('/contacts/export/all', async (req, res) => {
    try {
        console.log('📥 Exportando TODOS os contatos...');
        
        const contacts = await database.query(`
            SELECT phone, name, instance, saved_at, product
            FROM contacts 
            ORDER BY instance, saved_at DESC
        `);
        
        if (contacts.rows.length === 0) {
            return res.status(404).json({ 
                success: false, 
                message: 'Nenhum contato encontrado' 
            });
        }
        
        // CSV com instância no nome
        let csv = 'Name,Phone 1 - Value,Notes\n';
        
        contacts.rows.forEach(contact => {
            const date = getBrazilTime('DD/MM/YYYY HH:mm');
            const name = `${contact.name} - ${contact.instance}`;
            const notes = `Produto: ${contact.product} | Salvo: ${date}`;
            csv += `"${name}","${contact.phone}","${notes}"\n`;
        });
        
        const filename = `todos_contatos_${getBrazilTime('YYYY-MM-DD')}.csv`;
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.send(csv);
        
        console.log(`✅ ${contacts.rows.length} contatos exportados (todos)`);
        
    } catch (error) {
        console.error(`❌ Erro exportar todos: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * ENDPOINTS ADMINISTRATIVOS
 */

// Dashboard principal
app.get('/', (req, res) => {
    const htmlPath = path.join(__dirname, 'dashboard.html');
    if (fs.existsSync(htmlPath)) {
        res.sendFile(htmlPath);
    } else {
        res.send(getDashboardHTML());
    }
});

// Status do sistema
app.get('/status', async (req, res) => {
    try {
        // Converter Map para Array para JSON
        const conversationsArray = Array.from(conversations.values()).map(conv => ({
            ...conv,
            createdAt: conv.createdAt.toISOString(),
            lastActivity: conv.lastActivity.toISOString(),
            created_at_brazil: getBrazilTime('DD/MM/YYYY HH:mm:ss', conv.createdAt),
            last_activity_brazil: getBrazilTime('DD/MM/YYYY HH:mm:ss', conv.lastActivity)
        }));
        
        // Estatísticas por status
        const pendingPix = conversationsArray.filter(c => c.status === 'pix_pending').length;
        const approved = conversationsArray.filter(c => c.status === 'approved').length;
        const completed = conversationsArray.filter(c => c.status === 'completed').length;
        const convertidos = conversationsArray.filter(c => c.status === 'convertido').length;
        const timeout = conversationsArray.filter(c => c.status === 'timeout').length;
        
        // Instâncias com distribuição
        const instanceDistribution = {};
        conversationsArray.forEach(conv => {
            if (!instanceDistribution[conv.instance]) {
                instanceDistribution[conv.instance] = 0;
            }
            instanceDistribution[conv.instance]++;
        });
        
        res.json({
            system_status: 'online',
            version: '2.0',
            timestamp: new Date().toISOString(),
            brazil_time: getBrazilTime(),
            uptime: Math.floor(process.uptime()),
            
            stats: {
                pending_pix: pendingPix,
                active_conversations: conversations.size,
                approved_sales: approved,
                completed_sales: completed,
                converted_sales: convertidos,
                timeout_sales: timeout,
                contacts_saved: systemStats.contactsSaved,
                total_events: systemStats.totalEvents,
                successful_events: systemStats.successfulEvents,
                failed_events: systemStats.failedEvents,
                success_rate: systemStats.totalEvents > 0 
                    ? ((systemStats.successfulEvents / systemStats.totalEvents) * 100).toFixed(2) + '%'
                    : '0%'
            },
            
            config: {
                n8n_webhook_url: CONFIG.N8N_WEBHOOK_URL,
                evolution_api_url: CONFIG.EVOLUTION_API_URL,
                pix_timeout: CONFIG.PIX_TIMEOUT,
                pix_timeout_minutes: Math.round(CONFIG.PIX_TIMEOUT / 60000),
                timezone: 'America/Bahia'
            },
            
            conversations: conversationsArray,
            instance_distribution: instanceDistribution,
            pending_timeouts: pendingTimeouts.size,
            
            corrections: [
                'Sistema híbrido: memória + banco',
                'Contatos automáticos funcionando', 
                'Timezone Bahia correto',
                'Fluxo resposta_01, resposta_02, resposta_03',
                'PIX → timeout → convertido funcionando'
            ]
        });
        
    } catch (error) {
        console.error(`❌ Erro status: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Eventos recentes (últimas 24h na memória)
app.get('/events', async (req, res) => {
    try {
        const { limit = 50 } = req.query;
        
        // Eventos baseados nas conversas em memória
        const events = [];
        const now = new Date();
        const oneDayAgo = new Date(now - 24 * 60 * 60 * 1000);
        
        for (const conv of conversations.values()) {
            if (conv.createdAt > oneDayAgo) {
                events.push({
                    id: conv.id,
                    type: conv.status === 'approved' ? 'venda_aprovada' : 
                          conv.status === 'pix_pending' ? 'pix_gerado' :
                          conv.status === 'timeout' ? 'pix_timeout' :
                          conv.status === 'convertido' ? 'convertido' : 'evento',
                    date: getBrazilTime('DD/MM/YYYY', conv.createdAt),
                    time: getBrazilTime('HH:mm:ss', conv.createdAt),
                    clientName: getFirstName(conv.clientName),
                    clientPhone: conv.phone,
                    orderCode: conv.orderCode,
                    product: conv.product,
                    status: 'success',
                    instance: conv.instance,
                    responses: conv.responseCount,
                    amount: conv.amount
                });
            }
        }
        
        // Ordenar por data/hora (mais recente primeiro)
        events.sort((a, b) => new Date(b.date + ' ' + b.time) - new Date(a.date + ' ' + a.time));
        
        res.json({
            events: events.slice(0, parseInt(limit)),
            total: events.length,
            brazil_time: getBrazilTime()
        });
        
    } catch (error) {
        console.error(`❌ Erro events: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Health check
app.get('/health', (req, res) => {
    res.json({
        status: 'online',
        version: '2.0',
        timestamp: new Date().toISOString(),
        brazil_time: getBrazilTime(),
        database: database ? 'connected' : 'disconnected',
        memory_conversations: conversations.size,
        pending_timeouts: pendingTimeouts.size,
        system_stats: systemStats
    });
});

// Dashboard HTML embutido (caso arquivo não exista)
function getDashboardHTML() {
    return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <title>Cérebro v2.0 - Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .header { background: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
        .stat-card { background: white; padding: 20px; border-radius: 10px; text-align: center; }
        .stat-value { font-size: 2em; font-weight: bold; color: #007bff; }
        .btn { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; margin: 10px; }
        .btn:hover { background: #0056b3; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🧠 Cérebro de Atendimento v2.0</h1>
        <p>Sistema híbrido funcionando - <span id="current-time"></span></p>
        <button class="btn" onclick="window.location.reload()">🔄 Atualizar</button>
        <button class="btn" onclick="window.open('/contacts/export/all')">📥 Exportar Contatos</button>
    </div>
    <div class="stats">
        <div class="stat-card">
            <div class="stat-value" id="conversations">0</div>
            <div>Conversas Ativas</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="pending">0</div>
            <div>PIX Pendentes</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="contacts">0</div>
            <div>Contatos Salvos</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="success-rate">0%</div>
            <div>Taxa de Sucesso</div>
        </div>
    </div>
    
    <script>
        function updateTime() {
            document.getElementById('current-time').textContent = new Date().toLocaleString('pt-BR', {timeZone: 'America/Bahia'});
        }
        
        async function loadStats() {
            try {
                const [status, contacts] = await Promise.all([
                    fetch('/status').then(r => r.json()),
                    fetch('/contacts/stats').then(r => r.json())
                ]);
                
                document.getElementById('conversations').textContent = status.stats.active_conversations;
                document.getElementById('pending').textContent = status.stats.pending_pix;
                document.getElementById('contacts').textContent = contacts.total_contacts;
                document.getElementById('success-rate').textContent = status.stats.success_rate;
                
            } catch (error) {
                console.error('Erro ao carregar stats:', error);
            }
        }
        
        updateTime();
        loadStats();
        setInterval(updateTime, 1000);
        setInterval(loadStats, 30000);
    </script>
</body>
</html>`;
}

/**
 * LIMPEZA E MANUTENÇÃO
 */

// Limpar conversas antigas da memória (executar periodicamente)
function cleanupOldConversations() {
    const now = new Date();
    const twoDaysAgo = new Date(now - 48 * 60 * 60 * 1000); // 48 horas
    
    let cleaned = 0;
    
    for (const [phone, conversation] of conversations) {
        // Remover conversas antigas e completadas/timeout
        if (conversation.lastActivity < twoDaysAgo || 
            (conversation.status === 'completed' && conversation.lastActivity < new Date(now - 6 * 60 * 60 * 1000)) ||
            (conversation.status === 'timeout' && conversation.lastActivity < new Date(now - 6 * 60 * 60 * 1000))) {
            
            conversations.delete(phone);
            cleaned++;
        }
    }
    
    if (cleaned > 0) {
        console.log(`🧹 Limpeza: ${cleaned} conversas antigas removidas da memória`);
    }
}

// Executar limpeza a cada 30 minutos
setInterval(cleanupOldConversations, 30 * 60 * 1000);

/**
 * INICIALIZAÇÃO
 */
async function initializeSystem() {
    try {
        console.log('🧠 Inicializando Cérebro de Atendimento v2.0...');
        
        // Conectar banco
        await connectDatabase();
        
        // Verificar/criar tabela de contatos
        try {
            await database.query(`
                CREATE TABLE IF NOT EXISTS contacts (
                    id SERIAL PRIMARY KEY,
                    phone VARCHAR(20) NOT NULL,
                    name VARCHAR(10) NOT NULL,
                    instance VARCHAR(10) NOT NULL,
                    product VARCHAR(10),
                    conversation_id INTEGER,
                    saved_at TIMESTAMP DEFAULT NOW(),
                    created_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT unique_phone_date UNIQUE(phone, name)
                );
            `);
            console.log('✅ Tabela de contatos verificada/criada');
        } catch (tableError) {
            console.warn(`⚠️ Aviso tabela contatos: ${tableError.message}`);
        }
        
        console.log('✅ Sistema v2.0 inicializado com sucesso');
        console.log(`🎯 N8N Webhook: ${CONFIG.N8N_WEBHOOK_URL}`);
        console.log(`📱 Evolution API: ${CONFIG.EVOLUTION_API_URL}`);
        console.log(`⏰ Timezone: America/Bahia (${getBrazilTime()})`);
        
    } catch (error) {
        console.error(`❌ Erro crítico na inicialização: ${error.message}`);
        process.exit(1);
    }
}

// Tratamento de erros
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection:', reason);
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    process.exit(1);
});

process.on('SIGINT', () => {
    console.log('🔄 Finalizando sistema...');
    if (database) database.end();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('🔄 Finalizando sistema...');
    if (database) database.end();
    process.exit(0);
});

// Iniciar servidor
initializeSystem().then(() => {
    app.listen(PORT, () => {
        console.log(`🚀 Servidor rodando na porta ${PORT}`);
        console.log(`📊 Dashboard: https://cerebro-atendimento.flowzap.fun`);
        console.log(`📡 Webhook Perfect: https://cerebro-atendimento.flowzap.fun/webhook/perfect`);
        console.log(`📱 Webhook Evolution: https://cerebro-atendimento.flowzap.fun/webhook/evolution`);
        console.log(`✅ Webhook N8N Confirm: https://cerebro-atendimento.flowzap.fun/webhook/n8n-confirm`);
        console.log(`💳 Check Payment: https://cerebro-atendimento.flowzap.fun/check-payment/:orderId`);
        console.log(`📥 Exportar Contatos: https://cerebro-atendimento.flowzap.fun/contacts/export/all`);
        console.log(`\n✅ SISTEMA v2.0 ONLINE E FUNCIONANDO!`);
        console.log(`🎯 Principais melhorias:`);
        console.log(`   ✅ Sistema híbrido: memória + PostgreSQL`);
        console.log(`   ✅ Contatos automáticos após resposta_01`);
        console.log(`   ✅ Timezone Bahia/Brasília correto`);
        console.log(`   ✅ Normalização telefone corrigida`);
        console.log(`   ✅ Fluxo resposta_01 → resposta_02 → resposta_03`);
        console.log(`   ✅ PIX timeout → convertido funcionando`);
        console.log(`   ✅ Exportação Google Contacts`);
    });
});

// Executar limpeza inicial após 1 minuto
setTimeout(cleanupOldConversations, 60000);
