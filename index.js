/**
 * CÉREBRO DE ATENDIMENTO v3.0 - SISTEMA COMPLETAMENTE REFATORADO
 * 
 * ✅ Estrutura melhorada e organizada
 * ✅ Sistema de chaves consistente
 * ✅ Controle de funil robusto
 * ✅ Debug aprimorado
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

// ============================================
// CONFIGURAÇÃO DO SERVIDOR
// ============================================
const app = express();
const PORT = process.env.PORT || 3000;

// Middlewares
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ============================================
// CONFIGURAÇÕES DO SISTEMA
// ============================================
const CONFIG = {
    PIX_TIMEOUT: parseInt(process.env.PIX_TIMEOUT) || 420000, // 7 minutos
    N8N_WEBHOOK_URL: process.env.N8N_WEBHOOK_URL,
    EVOLUTION_API_URL: process.env.EVOLUTION_API_URL,
    MAX_RETRY_ATTEMPTS: parseInt(process.env.MAX_RETRY_ATTEMPTS) || 3,
    TIMEZONE: 'America/Bahia'
};

// ============================================
// MAPEAMENTOS
// ============================================
const PRODUCT_MAPPING = {
    'PPLQQM9AP': 'FAB',
    'PPLQQMAGU': 'FAB', 
    'PPLQQMADF': 'FAB',
    'PPLQQN0FT': 'NAT',
    'PPLQQMSFH': 'CS',
    'PPLQQMSFI': 'CS'
};

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

// ============================================
// ARMAZENAMENTO GLOBAL
// ============================================
let database = null;
let conversations = new Map();
let pendingTimeouts = new Map();
let instanceCounter = 0;
let systemStats = {
    totalEvents: 0,
    successfulEvents: 0,
    failedEvents: 0,
    contactsSaved: 0,
    startTime: new Date()
};

// ============================================
// CONEXÃO COM BANCO DE DADOS
// ============================================
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
        
        const client = await database.connect();
        const result = await client.query('SELECT NOW() as current_time');
        client.release();
        
        console.log('✅ PostgreSQL conectado');
        console.log(`📅 Hora do servidor: ${result.rows[0].current_time}`);
        
        // Criar tabelas necessárias
        await createTables();
        
    } catch (error) {
        console.error(`❌ Erro PostgreSQL: ${error.message}`);
        process.exit(1);
    }
}

async function createTables() {
    try {
        // Tabela de conversas
        await database.query(`
            CREATE TABLE IF NOT EXISTS conversations (
                id SERIAL PRIMARY KEY,
                phone VARCHAR(20) NOT NULL,
                order_code VARCHAR(50) UNIQUE NOT NULL,
                product VARCHAR(10),
                status VARCHAR(20),
                instance_name VARCHAR(20),
                amount DECIMAL(10,2),
                pix_url TEXT,
                client_name VARCHAR(255),
                responses_count INTEGER DEFAULT 0,
                last_response_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        `);
        
        // Tabela de contatos
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
            )
        `);
        
        console.log('✅ Tabelas verificadas/criadas');
        
    } catch (error) {
        console.warn(`⚠️ Aviso ao criar tabelas: ${error.message}`);
    }
}

// ============================================
// FUNÇÕES UTILITÁRIAS
// ============================================

// Horário Brasília/Bahia
function getBrazilTime(format = 'DD/MM/YYYY HH:mm:ss', date = null) {
    return moment(date || new Date()).tz(CONFIG.TIMEZONE).format(format);
}

// Extrair primeiro nome
function getFirstName(fullName) {
    return fullName ? fullName.split(' ')[0].trim() : 'Cliente';
}

// Obter produto pelo código do plano
function getProductByPlanCode(planCode) {
    return PRODUCT_MAPPING[planCode] || 'UNKNOWN';
}

// ============================================
// SISTEMA DE NORMALIZAÇÃO DE TELEFONE
// ============================================

/**
 * Normaliza o número de telefone para formato padrão
 * Sempre retorna: 5511999999999 (13 dígitos)
 */
function normalizePhoneNumber(phone) {
    if (!phone) return '';
    
    let cleaned = String(phone).trim().replace(/\D/g, '');
    
    // Caso 1: Já tem 13 dígitos com 55
    if (cleaned.length === 13 && cleaned.startsWith('55')) {
        return cleaned;
    }
    
    // Caso 2: 12 dígitos (55 + DDD + 8 dígitos) - adicionar 9
    if (cleaned.length === 12 && cleaned.startsWith('55')) {
        const ddd = cleaned.substring(2, 4);
        const numero = cleaned.substring(4);
        
        // Se é celular (começa com 6,7,8,9), adicionar 9
        if (['6', '7', '8', '9'].includes(numero[0])) {
            return '55' + ddd + '9' + numero;
        }
        return cleaned;
    }
    
    // Caso 3: 11 dígitos (DDD + número com 9)
    if (cleaned.length === 11) {
        return '55' + cleaned;
    }
    
    // Caso 4: 10 dígitos (DDD + número sem 9)
    if (cleaned.length === 10) {
        const ddd = cleaned.substring(0, 2);
        const numero = cleaned.substring(2);
        
        // Se é celular, adicionar 9
        if (['6', '7', '8', '9'].includes(numero[0])) {
            return '55' + ddd + '9' + numero;
        }
        return '55' + cleaned;
    }
    
    // Caso 5: Número com código do país diferente ou formato inválido
    console.warn(`⚠️ Formato de telefone não reconhecido: ${phone} → ${cleaned}`);
    return cleaned;
}

/**
 * FUNÇÃO CRÍTICA: Sempre retorna a mesma chave para um telefone
 * Isso garante consistência no Map de conversas
 */
function getConversationKey(phone) {
    const normalized = normalizePhoneNumber(phone);
    console.log(`🔑 Chave de conversa: ${phone} → ${normalized}`);
    return normalized;
}

/**
 * Formata telefone do Perfect Pay
 */
function formatPhoneFromPerfectPay(extension, areaCode, number) {
    const full = (extension || '55') + (areaCode || '') + (number || '');
    return normalizePhoneNumber(full);
}

// ============================================
// SISTEMA DE INSTÂNCIAS (STICKY SESSION)
// ============================================

async function getInstanceForClient(clientPhone) {
    try {
        const normalizedPhone = normalizePhoneNumber(clientPhone);
        console.log(`🔍 Buscando instância para: ${normalizedPhone}`);
        
        // Verificar cache em memória primeiro
        const conversationKey = getConversationKey(clientPhone);
        const cachedConv = conversations.get(conversationKey);
        if (cachedConv && cachedConv.instance) {
            console.log(`💾 Cache: Cliente usa instância ${cachedConv.instance}`);
            return cachedConv.instance;
        }
        
        // Verificar no banco
        try {
            const result = await database.query(
                'SELECT instance_name FROM conversations WHERE phone = $1 ORDER BY created_at DESC LIMIT 1',
                [normalizedPhone]
            );
            
            if (result.rows.length > 0) {
                const instance = result.rows[0].instance_name;
                console.log(`💾 Banco: Cliente usa instância ${instance}`);
                return instance;
            }
        } catch (dbError) {
            console.warn(`⚠️ Erro ao buscar instância no banco: ${dbError.message}`);
        }
        
        // Atribuir nova instância (round-robin)
        const activeInstances = INSTANCES.filter(i => i.active);
        const instance = activeInstances[instanceCounter % activeInstances.length];
        instanceCounter++;
        
        console.log(`⚖️ Nova instância atribuída: ${instance.name}`);
        return instance.name;
        
    } catch (error) {
        console.error(`❌ Erro ao obter instância: ${error.message}`);
        return 'GABY01'; // Fallback
    }
}

// ============================================
// SISTEMA DE CONTATOS
// ============================================

/**
 * Verifica se deve salvar o contato baseado na mensagem
 */
function shouldSaveContact(messageContent) {
    if (!messageContent) return false;
    
    // Palavras que indicam desinteresse
    const stopKeywords = [
        'pare', 'parar', 'sair', 'cancelar', 'remover', 
        'não quero', 'não tenho interesse', 'remove', 'stop'
    ];
    
    const messageClean = messageContent.toLowerCase().trim();
    
    // Verificar palavras de parada
    const wantsToStop = stopKeywords.some(keyword => 
        messageClean.includes(keyword)
    );
    
    if (wantsToStop) {
        console.log(`🚫 Cliente não quer contato: "${messageContent.substring(0, 50)}..."`);
        return false;
    }
    
    // Mensagem muito curta
    if (messageContent.trim().length < 2) {
        console.log(`📵 Mensagem muito curta para salvar contato`);
        return false;
    }
    
    return true;
}

/**
 * Salva contato automaticamente após primeira resposta
 */
async function saveContactAutomatically(phone, instanceName, conversationData) {
    try {
        const normalizedPhone = normalizePhoneNumber(phone);
        const today = getBrazilTime('DD/MM');
        
        console.log(`📇 Salvando contato: ${normalizedPhone} | ${today} | ${instanceName}`);
        
        // Verificar se já existe para hoje
        const existing = await database.query(
            'SELECT id FROM contacts WHERE phone = $1 AND name = $2',
            [normalizedPhone, today]
        );
        
        if (existing.rows.length > 0) {
            console.log(`📇 Contato já existe para hoje: ${normalizedPhone}`);
            return { success: true, action: 'exists' };
        }
        
        // Salvar novo contato
        const result = await database.query(`
            INSERT INTO contacts (phone, name, instance, product, conversation_id) 
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        `, [
            normalizedPhone, 
            today, 
            instanceName, 
            conversationData.product || 'UNKNOWN',
            conversationData.id || null
        ]);
        
        const contactId = result.rows[0].id;
        systemStats.contactsSaved++;
        
        console.log(`✅ Contato salvo: ID ${contactId} | ${today} | ${normalizedPhone} | ${instanceName}`);
        
        return { 
            success: true, 
            action: 'saved', 
            contact_id: contactId,
            date: today
        };
        
    } catch (error) {
        if (error.message.includes('unique_phone_date')) {
            console.log(`📇 Contato duplicado ignorado`);
            return { success: true, action: 'duplicate' };
        }
        
        console.error(`❌ Erro ao salvar contato: ${error.message}`);
        return { success: false, error: error.message };
    }
}
// ============================================
// SISTEMA DE COMUNICAÇÃO COM N8N
// ============================================

/**
 * Envia evento para o N8N com retry automático
 */
async function sendToN8N(eventData, eventType, attempt = 1) {
    const maxAttempts = CONFIG.MAX_RETRY_ATTEMPTS;
    
    try {
        console.log(`📤 Enviando para N8N (${attempt}/${maxAttempts}): ${eventType}`);
        console.log(`🎯 URL: ${CONFIG.N8N_WEBHOOK_URL}`);
        console.log(`📦 Dados:`, JSON.stringify(eventData, null, 2));
        
        const response = await axios.post(CONFIG.N8N_WEBHOOK_URL, eventData, {
            headers: {
                'Content-Type': 'application/json',
                'User-Agent': 'Cerebro-v3.0'
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
            const delay = attempt * 2000; // 2s, 4s, 6s
            console.log(`🔄 Tentando novamente em ${delay/1000}s...`);
            
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

// ============================================
// PROCESSAMENTO DE VENDA APROVADA
// ============================================

async function handleApprovedSale(orderCode, phoneNumber, firstName, fullName, product, amount) {
    try {
        console.log(`💰 VENDA APROVADA: ${orderCode} | ${product} | ${firstName}`);
        
        // Obter instância e chave de conversa
        const instanceName = await getInstanceForClient(phoneNumber);
        const conversationKey = getConversationKey(phoneNumber);
        
        console.log(`🔑 Salvando conversa aprovada com chave: ${conversationKey}`);
        
        // Cancelar timeout PIX se existir
        if (pendingTimeouts.has(orderCode)) {
            clearTimeout(pendingTimeouts.get(orderCode));
            pendingTimeouts.delete(orderCode);
            console.log(`🗑️ Timeout PIX cancelado: ${orderCode}`);
        }
        
        // Criar objeto de conversa
        const conversation = {
            phone: conversationKey,
            orderCode: orderCode,
            product: product,
            status: 'approved',
            instance: instanceName,
            amount: amount,
            clientName: fullName,
            createdAt: new Date(),
            lastActivity: new Date(),
            responseCount: 0,
            pixUrl: '',
            id: Date.now(),
            // Flags de controle de funil
            waitingConfirmation: false,
            pendingStep: null,
            funilInProgress: false
        };
        
        // Salvar em memória (cache)
        conversations.set(conversationKey, conversation);
        
        // Salvar no banco
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
            `, [conversationKey, orderCode, product, instanceName, amount, fullName]);
            
            console.log(`💾 Venda aprovada salva no banco`);
        } catch (dbError) {
            console.warn(`⚠️ Erro ao salvar no banco: ${dbError.message}`);
        }
        
        // Enviar evento para N8N
        const eventData = {
            event_type: 'venda_aprovada',
            produto: product,
            instancia: instanceName,
            evento_origem: 'aprovada',
            cliente: {
                nome: firstName,
                telefone: conversationKey,
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
        
        console.log(`✅ Venda aprovada processada completamente: ${orderCode}`);
        return true;
        
    } catch (error) {
        console.error(`❌ Erro ao processar venda aprovada: ${error.message}`);
        return false;
    }
}

// ============================================
// PROCESSAMENTO DE PIX PENDENTE
// ============================================

async function handlePendingPix(orderCode, phoneNumber, firstName, fullName, product, amount, pixUrl) {
    try {
        console.log(`⏰ PIX GERADO: ${orderCode} | ${product} | ${firstName}`);
        
        // Obter instância e chave de conversa
        const instanceName = await getInstanceForClient(phoneNumber);
        const conversationKey = getConversationKey(phoneNumber);
        
        console.log(`🔑 Salvando PIX pendente com chave: ${conversationKey}`);
        
        // Cancelar timeout anterior se existir
        if (pendingTimeouts.has(orderCode)) {
            clearTimeout(pendingTimeouts.get(orderCode));
            pendingTimeouts.delete(orderCode);
            console.log(`🗑️ Timeout anterior cancelado`);
        }
        
        // Criar objeto de conversa
        const conversation = {
            phone: conversationKey,
            orderCode: orderCode,
            product: product,
            status: 'pix_pending',
            instance: instanceName,
            amount: amount,
            clientName: fullName,
            createdAt: new Date(),
            lastActivity: new Date(),
            responseCount: 0,
            pixUrl: pixUrl,
            id: Date.now(),
            // Flags de controle de funil
            waitingConfirmation: false,
            pendingStep: null,
            funilInProgress: false
        };
        
        // Salvar em memória (cache)
        conversations.set(conversationKey, conversation);
        
        // Salvar no banco
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
            `, [conversationKey, orderCode, product, instanceName, amount, pixUrl, fullName]);
            
            console.log(`💾 PIX pendente salvo no banco`);
        } catch (dbError) {
            console.warn(`⚠️ Erro ao salvar no banco: ${dbError.message}`);
        }
        
        // Criar timeout de 7 minutos
        const timeout = setTimeout(async () => {
            console.log(`⏰ TIMEOUT PIX ACIONADO: ${orderCode}`);
            pendingTimeouts.delete(orderCode);
            await handlePixTimeout(orderCode, conversation);
        }, CONFIG.PIX_TIMEOUT);
        
        pendingTimeouts.set(orderCode, timeout);
        
        console.log(`⏰ Timeout criado: ${Math.round(CONFIG.PIX_TIMEOUT/60000)} minutos para ${orderCode}`);
        console.log(`✅ PIX pendente processado completamente: ${orderCode}`);
        return true;
        
    } catch (error) {
        console.error(`❌ Erro ao processar PIX pendente: ${error.message}`);
        return false;
    }
}

// ============================================
// PROCESSAMENTO DE TIMEOUT PIX
// ============================================

async function handlePixTimeout(orderCode, conversation) {
    try {
        console.log(`⏰ Processando timeout PIX: ${orderCode}`);
        
        // Verificar se ainda está pendente
        const currentConv = conversations.get(conversation.phone);
        if (!currentConv || currentConv.status !== 'pix_pending') {
            console.log(`ℹ️ PIX ${orderCode} não está mais pendente, cancelando timeout`);
            return;
        }
        
        // Atualizar status para timeout
        currentConv.status = 'timeout';
        currentConv.lastActivity = new Date();
        conversations.set(conversation.phone, currentConv);
        
        // Atualizar no banco
        try {
            await database.query(
                'UPDATE conversations SET status = $1, updated_at = NOW() WHERE order_code = $2',
                ['timeout', orderCode]
            );
            console.log(`💾 Status timeout salvo no banco`);
        } catch (dbError) {
            console.warn(`⚠️ Erro ao atualizar banco: ${dbError.message}`);
        }
        
        // Enviar evento para N8N
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
            timeout_minutos: Math.round(CONFIG.PIX_TIMEOUT/60000),
            timestamp: new Date().toISOString(),
            brazil_time: getBrazilTime(),
            conversation_id: conversation.id
        };
        
        await sendToN8N(eventData, 'pix_timeout');
        
        console.log(`✅ Timeout PIX processado: ${orderCode}`);
        return true;
        
    } catch (error) {
        console.error(`❌ Erro ao processar timeout PIX: ${error.message}`);
        return false;
    }
}

// ============================================
// PROCESSAMENTO DE CONVERSÃO PIX
// ============================================

async function sendConversionEvent(conversation, messageContent) {
    try {
        const firstName = getFirstName(conversation.clientName);
        
        console.log(`🎯 Enviando evento de conversão: ${conversation.orderCode}`);
        
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
            console.log(`✅ Evento de conversão enviado: ${conversation.orderCode}`);
        }
        
        return success;
        
    } catch (error) {
        console.error(`❌ Erro ao enviar evento de conversão: ${error.message}`);
        return false;
    }
}

// ============================================
// VERIFICAÇÃO DE STATUS DE PAGAMENTO
// ============================================

async function checkPaymentStatus(orderCode) {
    try {
        // Verificar na memória primeiro (mais rápido)
        for (const [phone, conv] of conversations) {
            if (conv.orderCode === orderCode) {
                const isPaid = conv.status === 'approved' || conv.status === 'completed' || conv.status === 'convertido';
                if (isPaid) {
                    console.log(`💰 Pagamento confirmado (cache): ${orderCode}`);
                    return true;
                }
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
                const isPaid = status === 'approved' || status === 'completed' || status === 'convertido';
                if (isPaid) {
                    console.log(`💰 Pagamento confirmado (banco): ${orderCode}`);
                }
                return isPaid;
            }
        } catch (dbError) {
            console.warn(`⚠️ Erro ao verificar pagamento no banco: ${dbError.message}`);
        }
        
        return false;
        
    } catch (error) {
        console.error(`❌ Erro ao verificar status de pagamento: ${error.message}`);
        return false;
    }
}

// ============================================
// PROCESSAMENTO DE MENSAGEM DO SISTEMA
// ============================================

async function handleSystemMessage(clientNumber, messageContent, instanceName) {
    try {
        const conversationKey = getConversationKey(clientNumber);
        console.log(`📤 Mensagem do sistema para: ${conversationKey}`);
        
        // Buscar conversa
        const conversation = conversations.get(conversationKey);
        if (conversation) {
            conversation.lastActivity = new Date();
            conversations.set(conversationKey, conversation);
            console.log(`⏰ Última atividade atualizada`);
        }
        
    } catch (error) {
        console.error(`❌ Erro ao processar mensagem do sistema: ${error.message}`);
    }
}

// ============================================
// PROCESSAMENTO DE RESPOSTA DO CLIENTE - PARTE CRÍTICA
// ============================================

async function handleClientResponse(clientNumber, messageContent, instanceName, messageData) {
    try {
        const conversationKey = getConversationKey(clientNumber);
        console.log(`📥 RESPOSTA CLIENTE: ${conversationKey} | "${messageContent.substring(0, 50)}..."`);
        
        // Buscar conversa ativa
        const conversation = conversations.get(conversationKey);
        
        if (!conversation) {
            console.log(`⚠️ Cliente ${conversationKey} não encontrado nas conversas ativas`);
            console.log(`📋 Chaves ativas: ${Array.from(conversations.keys()).join(', ')}`);
            return;
        }
        
        // VERIFICAÇÃO CRÍTICA 1: Se o funil está em progresso, ignorar mensagem
        if (conversation.funilInProgress) {
            console.log(`🚧 Cliente ${conversationKey} - funil em progresso - ignorando mensagem`);
            console.log(`   Status: ${conversation.status}`);
            console.log(`   Etapa atual: ${conversation.responseCount}/3`);
            console.log(`   Aguardando confirmação do N8N para continuar`);
            return;
        }
        
        // VERIFICAÇÃO CRÍTICA 2: Se está aguardando confirmação do N8N, ignorar
        if (conversation.waitingConfirmation) {
            console.log(`⏳ Cliente ${conversationKey} aguardando confirmação N8N - ignorando mensagem`);
            return;
        }
        
        // Verificar se PIX foi pago durante o fluxo
        if (conversation.status === 'pix_pending') {
            const isPaid = await checkPaymentStatus(conversation.orderCode);
            
            if (isPaid) {
                console.log(`🎉 PIX pago durante fluxo - processando conversão`);
                
                // Cancelar timeout PIX
                if (pendingTimeouts.has(conversation.orderCode)) {
                    clearTimeout(pendingTimeouts.get(conversation.orderCode));
                    pendingTimeouts.delete(conversation.orderCode);
                    console.log(`🗑️ Timeout PIX cancelado por pagamento`);
                }
                
                // Atualizar status
                conversation.status = 'convertido';
                conversation.lastActivity = new Date();
                conversations.set(conversationKey, conversation);
                
                // Salvar contato se for primeira resposta
                if (conversation.responseCount === 0 && shouldSaveContact(messageContent)) {
                    await saveContactAutomatically(conversationKey, conversation.instance, conversation);
                }
                
                // Enviar evento de conversão
                await sendConversionEvent(conversation, messageContent);
                return;
            }
        }
        
        // Determinar próximo passo baseado em responseCount
        const nextStep = conversation.responseCount + 1;
        
        if (nextStep > 3) {
            console.log(`✅ Funil completo - ${conversationKey} já recebeu todas as 3 respostas`);
            return;
        }
        
        console.log(`📋 Iniciando envio da resposta_0${nextStep} para ${conversationKey}`);
        
        // SALVAR CONTATO NA PRIMEIRA RESPOSTA
        if (nextStep === 1 && shouldSaveContact(messageContent)) {
            const contactResult = await saveContactAutomatically(conversationKey, conversation.instance, conversation);
            if (contactResult.success) {
                console.log(`📇 Contato salvo automaticamente na primeira resposta`);
            }
        }
        
        // MARCAR CONVERSA COMO BLOQUEADA ATÉ CONFIRMAÇÃO
        conversation.waitingConfirmation = true;
        conversation.funilInProgress = true;
        conversation.pendingStep = nextStep;
        conversation.lastActivity = new Date();
        conversations.set(conversationKey, conversation);
        
        console.log(`🔒 Conversa bloqueada - aguardando funil completo`);
        console.log(`   waitingConfirmation: true`);
        console.log(`   funilInProgress: true`);
        console.log(`   pendingStep: ${nextStep}`);
        
        // Preparar dados para N8N
        const firstName = getFirstName(conversation.clientName);
        const eventData = {
            event_type: `resposta_0${nextStep}`,
            produto: conversation.product,
            instancia: conversation.instance,
            evento_origem: conversation.status === 'approved' ? 'aprovada' : 'pix',
            cliente: {
                telefone: conversationKey,
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
            console.log(`✅ Resposta_0${nextStep} enviada - aguardando execução completa do funil`);
        } else {
            console.error(`❌ Falha ao enviar resposta_0${nextStep} - liberando conversa`);
            
            // Se falhou, liberar a conversa para não travar permanentemente
            conversation.waitingConfirmation = false;
            conversation.funilInProgress = false;
            conversation.pendingStep = null;
            conversations.set(conversationKey, conversation);
            
            console.log(`🔓 Conversa liberada por falha no envio`);
        }
        
    } catch (error) {
        console.error(`❌ Erro ao processar resposta do cliente: ${error.message}`);
    }
}

// ============================================
// WEBHOOKS PRINCIPAIS
// ============================================

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
        
        console.log(`\n📥 WEBHOOK PERFECT PAY`);
        console.log(`   Pedido: ${orderCode}`);
        console.log(`   Status: ${status}`);
        console.log(`   Produto: ${product}`);
        console.log(`   Cliente: ${fullName}`);
        console.log(`   Telefone: ${phoneNumber}`);
        
        systemStats.totalEvents++;
        
        // Processar baseado no status
        if (status === 'approved') {
            await handleApprovedSale(orderCode, phoneNumber, firstName, fullName, product, amount);
        } else if (status === 'pending') {
            await handlePendingPix(orderCode, phoneNumber, firstName, fullName, product, amount, pixUrl);
        } else {
            console.log(`ℹ️ Status ignorado: ${status}`);
        }
        
        res.status(200).json({ 
            success: true, 
            message: 'Perfect Pay processado',
            order_code: orderCode,
            product: product,
            phone_normalized: getConversationKey(phoneNumber)
        });
        
    } catch (error) {
        console.error(`❌ Erro no webhook Perfect Pay: ${error.message}`);
        systemStats.failedEvents++;
        res.status(500).json({ success: false, error: error.message });
    }
});

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
        
        const clientNumber = remoteJid.replace('@s.whatsapp.net', '');
        
        console.log(`\n📱 WEBHOOK EVOLUTION`);
        console.log(`   De: ${fromMe ? 'Sistema' : 'Cliente'}`);
        console.log(`   Número: ${clientNumber}`);
        console.log(`   Instância: ${instanceName}`);
        
        systemStats.totalEvents++;
        
        if (fromMe) {
            await handleSystemMessage(clientNumber, messageContent, instanceName);
        } else {
            await handleClientResponse(clientNumber, messageContent, instanceName, messageData);
        }
        
        res.status(200).json({ 
            success: true, 
            message: 'Evolution processado',
            client_number: getConversationKey(clientNumber),
            from_me: fromMe
        });
        
    } catch (error) {
        console.error(`❌ Erro no webhook Evolution: ${error.message}`);
        systemStats.failedEvents++;
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * WEBHOOK N8N CONFIRM - CRÍTICO PARA LIBERAR FUNIL
 */
app.post('/webhook/n8n-confirm', async (req, res) => {
    try {
        const { tipo_mensagem, telefone, instancia, funil_completo } = req.body;
        
        // SEMPRE normalizar o telefone
        const conversationKey = getConversationKey(telefone);
        
        console.log(`\n📨 WEBHOOK N8N CONFIRM`);
        console.log(`   Tipo: ${tipo_mensagem}`);
        console.log(`   Telefone: ${telefone} → ${conversationKey}`);
        console.log(`   Instância: ${instancia}`);
        console.log(`   Funil completo: ${funil_completo} (tipo: ${typeof funil_completo})`);
        
        // Buscar conversa
        const conversation = conversations.get(conversationKey);
        
        if (!conversation) {
            console.warn(`⚠️ Conversa não encontrada para confirmação: ${conversationKey}`);
            console.log(`📋 Chaves disponíveis: ${Array.from(conversations.keys()).slice(0, 5).join(', ')}...`);
            
            return res.json({ 
                success: false, 
                message: 'Conversa não encontrada',
                chave_buscada: conversationKey,
                total_conversas: conversations.size
            });
        }
        
        console.log(`📊 Estado atual da conversa:`);
        console.log(`   Cliente: ${conversation.clientName}`);
        console.log(`   Pedido: ${conversation.orderCode}`);
        console.log(`   Status: ${conversation.status}`);
        console.log(`   Respostas: ${conversation.responseCount}/3`);
        console.log(`   waitingConfirmation: ${conversation.waitingConfirmation}`);
        console.log(`   funilInProgress: ${conversation.funilInProgress}`);
        console.log(`   pendingStep: ${conversation.pendingStep}`);
        
        // PROCESSAR CONFIRMAÇÃO BASEADO EM funil_completo
        if (funil_completo === true || funil_completo === 'true' || funil_completo === "true") {
            console.log(`✅ FUNIL COMPLETO - Liberando conversa`);
            
            // Atualizar contador de respostas
            if (conversation.pendingStep) {
                conversation.responseCount = conversation.pendingStep;
                console.log(`📊 Respostas atualizadas: ${conversation.responseCount}/3`);
            }
            
            // LIMPAR TODAS AS FLAGS DE BLOQUEIO
            conversation.waitingConfirmation = false;
            conversation.funilInProgress = false;
            conversation.pendingStep = null;
            conversation.lastActivity = new Date();
            
            // Verificar se completou todas as respostas
            if (conversation.responseCount === 3) {
                conversation.status = 'completed';
                console.log(`🎯 Funil totalmente completo - todas as 3 respostas enviadas`);
                
                // Atualizar no banco
                try {
                    await database.query(
                        'UPDATE conversations SET status = $1, responses_count = $2, updated_at = NOW() WHERE order_code = $3',
                        ['completed', 3, conversation.orderCode]
                    );
                } catch (dbError) {
                    console.warn(`⚠️ Erro ao atualizar banco: ${dbError.message}`);
                }
            } else {
                // Atualizar apenas o contador no banco
                try {
                    await database.query(
                        'UPDATE conversations SET responses_count = $1, last_response_at = NOW(), updated_at = NOW() WHERE order_code = $2',
                        [conversation.responseCount, conversation.orderCode]
                    );
                } catch (dbError) {
                    console.warn(`⚠️ Erro ao atualizar banco: ${dbError.message}`);
                }
            }
            
            // Salvar conversa atualizada
            conversations.set(conversationKey, conversation);
            
            console.log(`🔓 CONVERSA LIBERADA - Cliente pode enviar próxima mensagem`);
            console.log(`   waitingConfirmation: false`);
            console.log(`   funilInProgress: false`);
            console.log(`   Próxima resposta será: resposta_0${conversation.responseCount + 1}`);
            
        } else {
            // Funil ainda em execução
            console.log(`⏳ Funil ainda em execução - mantendo bloqueio`);
            conversation.lastActivity = new Date();
            conversations.set(conversationKey, conversation);
        }
        
        res.json({ 
            success: true,
            message: funil_completo ? 'Funil completo - conversa liberada' : 'Funil em execução',
            funil_completo: funil_completo,
            pedido: conversation.orderCode,
            cliente: conversation.clientName,
            telefone_normalizado: conversationKey,
            respostas_atuais: conversation.responseCount,
            proxima_resposta: conversation.responseCount < 3 ? `resposta_0${conversation.responseCount + 1}` : 'completo',
            status_conversa: conversation.status,
            liberado_para_proxima: !conversation.funilInProgress && !conversation.waitingConfirmation,
            flags: {
                waitingConfirmation: conversation.waitingConfirmation,
                funilInProgress: conversation.funilInProgress,
                pendingStep: conversation.pendingStep
            }
        });
        
    } catch (error) {
        console.error(`❌ Erro no webhook N8N confirm: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================
// ENDPOINTS AUXILIARES PARA N8N
// ============================================

/**
 * Verificar status de pagamento
 */
app.get('/check-payment/:orderId', async (req, res) => {
    try {
        const { orderId } = req.params;
        
        console.log(`💳 Verificando pagamento: ${orderId}`);
        
        const isPaid = await checkPaymentStatus(orderId);
        
        res.json({ 
            status: isPaid ? 'paid' : 'pending',
            order_id: orderId,
            timestamp: getBrazilTime()
        });
        
    } catch (error) {
        console.error(`❌ Erro ao verificar pagamento: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Marcar conversa como completa
 */
app.post('/webhook/complete/:orderId', async (req, res) => {
    try {
        const { orderId } = req.params;
        
        console.log(`✅ Marcando como completo: ${orderId}`);
        
        // Buscar e atualizar conversa
        let found = false;
        for (const [phone, conv] of conversations) {
            if (conv.orderCode === orderId) {
                conv.status = 'completed';
                conv.lastActivity = new Date();
                conversations.set(phone, conv);
                found = true;
                console.log(`✅ Conversa marcada como completa em memória`);
                break;
            }
        }
        
        // Atualizar banco
        try {
            await database.query(
                'UPDATE conversations SET status = $1, updated_at = NOW() WHERE order_code = $2',
                ['completed', orderId]
            );
            console.log(`✅ Status completo salvo no banco`);
        } catch (dbError) {
            console.warn(`⚠️ Erro ao atualizar banco: ${dbError.message}`);
        }
        
        res.json({ 
            success: true, 
            message: 'Conversa marcada como completa',
            order_id: orderId,
            found_in_memory: found
        });
        
    } catch (error) {
        console.error(`❌ Erro ao marcar como completo: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});
// ============================================
// ENDPOINTS DE DEBUG
// ============================================

/**
 * Debug de conversa específica
 */
app.get('/debug/conversation/:phone', (req, res) => {
    const phone = req.params.phone;
    const conversationKey = getConversationKey(phone);
    const conversation = conversations.get(conversationKey);
    
    console.log(`🔍 Debug conversa: ${phone} → ${conversationKey}`);
    
    res.json({
        telefone_original: phone,
        chave_normalizada: conversationKey,
        conversa_encontrada: !!conversation,
        detalhes_conversa: conversation || null,
        total_conversas_ativas: conversations.size,
        chaves_ativas: Array.from(conversations.keys()).slice(0, 10)
    });
});

/**
 * Limpar flags travadas (emergência)
 */
app.post('/debug/clear-flags/:phone', (req, res) => {
    const phone = req.params.phone;
    const conversationKey = getConversationKey(phone);
    const conversation = conversations.get(conversationKey);
    
    if (conversation) {
        console.log(`🔧 Limpando flags de ${conversationKey}`);
        conversation.waitingConfirmation = false;
        conversation.funilInProgress = false;
        conversation.pendingStep = null;
        conversations.set(conversationKey, conversation);
        
        res.json({
            success: true,
            message: 'Flags limpas',
            conversa: conversation
        });
    } else {
        res.status(404).json({
            success: false,
            message: 'Conversa não encontrada'
        });
    }
});

// ============================================
// ENDPOINTS DE CONTATOS
// ============================================

/**
 * Estatísticas de contatos
 */
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
            total_contacts: parseInt(totalContacts.rows[0].total || 0),
            by_instance: stats.rows.map(row => ({
                instance: row.instance,
                total: parseInt(row.total),
                today: parseInt(row.today || 0),
                this_week: parseInt(row.this_week || 0),
                first_contact: row.first_contact,
                last_contact: row.last_contact
            }))
        });
        
    } catch (error) {
        console.error(`❌ Erro ao obter estatísticas de contatos: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Exportar contatos por instância
 */
app.get('/contacts/export/:instance', async (req, res) => {
    try {
        const { instance } = req.params;
        
        console.log(`📥 Exportando contatos da instância: ${instance}`);
        
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
        
        // Gerar CSV formato Google Contacts
        let csv = 'Name,Phone 1 - Value,Notes\n';
        
        contacts.rows.forEach(contact => {
            const date = getBrazilTime('DD/MM/YYYY HH:mm', contact.saved_at);
            const notes = `Instância: ${instance} | Produto: ${contact.product} | Salvo: ${date}`;
            csv += `"${contact.name}","${contact.phone}","${notes}"\n`;
        });
        
        const filename = `contatos_${instance.toLowerCase()}_${getBrazilTime('YYYY-MM-DD')}.csv`;
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.send(csv);
        
        console.log(`✅ ${contacts.rows.length} contatos exportados: ${instance}`);
        
    } catch (error) {
        console.error(`❌ Erro ao exportar contatos: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Exportar todos os contatos
 */
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
        
        // Gerar CSV com instância no nome
        let csv = 'Name,Phone 1 - Value,Notes\n';
        
        contacts.rows.forEach(contact => {
            const date = getBrazilTime('DD/MM/YYYY HH:mm', contact.saved_at);
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
        console.error(`❌ Erro ao exportar todos os contatos: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================
// ENDPOINTS ADMINISTRATIVOS
// ============================================

/**
 * Dashboard principal
 */
app.get('/', (req, res) => {
    const htmlPath = path.join(__dirname, 'dashboard.html');
    if (fs.existsSync(htmlPath)) {
        res.sendFile(htmlPath);
    } else {
        res.send(getDashboardHTML());
    }
});

/**
 * Status completo do sistema
 */
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
        const stats = {
            pending_pix: conversationsArray.filter(c => c.status === 'pix_pending').length,
            approved: conversationsArray.filter(c => c.status === 'approved').length,
            completed: conversationsArray.filter(c => c.status === 'completed').length,
            convertidos: conversationsArray.filter(c => c.status === 'convertido').length,
            timeout: conversationsArray.filter(c => c.status === 'timeout').length,
            blocked: conversationsArray.filter(c => c.funilInProgress || c.waitingConfirmation).length
        };
        
        // Distribuição por instância
        const instanceDistribution = {};
        conversationsArray.forEach(conv => {
            instanceDistribution[conv.instance] = (instanceDistribution[conv.instance] || 0) + 1;
        });
        
        res.json({
            system_status: 'online',
            version: '3.0',
            timestamp: new Date().toISOString(),
            brazil_time: getBrazilTime(),
            uptime: Math.floor(process.uptime()),
            
            stats: {
                pending_pix: stats.pending_pix,
                active_conversations: conversations.size,
                approved_sales: stats.approved,
                completed_sales: stats.completed,
                converted_sales: stats.convertidos,
                timeout_sales: stats.timeout,
                blocked_conversations: stats.blocked,
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
                pix_timeout_minutes: Math.round(CONFIG.PIX_TIMEOUT / 60000),
                timezone: CONFIG.TIMEZONE
            },
            
            conversations: conversationsArray,
            instance_distribution: instanceDistribution,
            pending_timeouts: pendingTimeouts.size,
            
            features: [
                'Sistema de chaves consistente',
                'Controle robusto de funil',
                'Debug aprimorado',
                'Contatos automáticos',
                'Timeout PIX configurável',
                'Retry automático N8N'
            ]
        });
        
    } catch (error) {
        console.error(`❌ Erro ao obter status: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Eventos recentes
 */
app.get('/events', async (req, res) => {
    try {
        const { limit = 50 } = req.query;
        
        const events = [];
        const now = new Date();
        const oneDayAgo = new Date(now - 24 * 60 * 60 * 1000);
        
        for (const conv of conversations.values()) {
            if (conv.createdAt > oneDayAgo) {
                events.push({
                    id: conv.id,
                    type: conv.status,
                    date: getBrazilTime('DD/MM/YYYY', conv.createdAt),
                    time: getBrazilTime('HH:mm:ss', conv.createdAt),
                    clientName: getFirstName(conv.clientName),
                    clientPhone: conv.phone,
                    orderCode: conv.orderCode,
                    product: conv.product,
                    instance: conv.instance,
                    responses: conv.responseCount,
                    amount: conv.amount,
                    blocked: conv.funilInProgress || conv.waitingConfirmation
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
        console.error(`❌ Erro ao obter eventos: ${error.message}`);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Health check
 */
app.get('/health', (req, res) => {
    res.json({
        status: 'online',
        version: '3.0',
        timestamp: new Date().toISOString(),
        brazil_time: getBrazilTime(),
        database: database ? 'connected' : 'disconnected',
        memory_conversations: conversations.size,
        pending_timeouts: pendingTimeouts.size,
        system_stats: systemStats
    });
});

// ============================================
// DASHBOARD HTML
// ============================================

function getDashboardHTML() {
    return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <title>Cérebro v3.0 - Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { 
            background: rgba(255,255,255,0.98); 
            padding: 30px; 
            border-radius: 15px; 
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        .header h1 { 
            color: #333; 
            margin-bottom: 10px;
            display: inline-flex;
            align-items: center;
            gap: 10px;
        }
        .version {
            background: linear-gradient(135deg, #48bb78, #38a169);
            color: white;
            padding: 5px 12px;
            border-radius: 12px;
            font-size: 0.8rem;
            font-weight: 600;
        }
        .stats { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px;
            margin-bottom: 20px;
        }
        .stat-card { 
            background: white; 
            padding: 20px; 
            border-radius: 12px; 
            text-align: center;
            box-shadow: 0 5px 20px rgba(0,0,0,0.08);
            transition: transform 0.3s ease;
        }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-value { 
            font-size: 2.5rem; 
            font-weight: bold; 
            color: #667eea;
            margin-bottom: 8px;
        }
        .stat-label {
            color: #666;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .btn { 
            background: linear-gradient(135deg, #667eea, #764ba2);
            color: white; 
            padding: 10px 20px; 
            border: none; 
            border-radius: 20px; 
            cursor: pointer;
            font-weight: 600;
            margin: 5px;
            transition: all 0.3s ease;
        }
        .btn:hover { 
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
        }
        .time { 
            color: #666; 
            margin: 10px 0;
            font-size: 0.95rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🧠 Cérebro de Atendimento <span class="version">v3.0</span></h1>
            <div class="time">Sistema rodando - <span id="current-time"></span></div>
            <div style="margin-top: 15px;">
                <button class="btn" onclick="window.location.reload()">🔄 Atualizar</button>
                <button class="btn" onclick="window.open('/status')">📊 Status Completo</button>
                <button class="btn" onclick="window.open('/contacts/export/all')">📥 Exportar Contatos</button>
                <button class="btn" onclick="window.open('/health')">❤️ Health Check</button>
            </div>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="conversations">0</div>
                <div class="stat-label">Conversas Ativas</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="pending">0</div>
                <div class="stat-label">PIX Pendentes</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="contacts">0</div>
                <div class="stat-label">Contatos Salvos</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="success-rate">0%</div>
                <div class="stat-label">Taxa de Sucesso</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="blocked">0</div>
                <div class="stat-label">Funis em Execução</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="completed">0</div>
                <div class="stat-label">Funis Completos</div>
            </div>
        </div>
    </div>
    
    <script>
        function updateTime() {
            document.getElementById('current-time').textContent = 
                new Date().toLocaleString('pt-BR', {timeZone: 'America/Bahia'});
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
                document.getElementById('blocked').textContent = status.stats.blocked_conversations || 0;
                document.getElementById('completed').textContent = status.stats.completed_sales;
                
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

// ============================================
// SISTEMA DE LIMPEZA E MANUTENÇÃO
// ============================================

/**
 * Limpar conversas antigas da memória
 */
function cleanupOldConversations() {
    const now = new Date();
    const twoDaysAgo = new Date(now - 48 * 60 * 60 * 1000);
    const sixHoursAgo = new Date(now - 6 * 60 * 60 * 1000);
    
    let cleaned = 0;
    
    for (const [phone, conversation] of conversations) {
        // Remover conversas muito antigas
        if (conversation.lastActivity < twoDaysAgo) {
            conversations.delete(phone);
            cleaned++;
            continue;
        }
        
        // Remover conversas completadas/timeout após 6 horas
        if ((conversation.status === 'completed' || conversation.status === 'timeout') && 
            conversation.lastActivity < sixHoursAgo) {
            conversations.delete(phone);
            cleaned++;
        }
    }
    
    if (cleaned > 0) {
        console.log(`🧹 Limpeza automática: ${cleaned} conversas antigas removidas`);
    }
}

// ============================================
// INICIALIZAÇÃO DO SISTEMA
// ============================================

async function initializeSystem() {
    try {
        console.log('============================================');
        console.log('🧠 CÉREBRO DE ATENDIMENTO v3.0');
        console.log('============================================');
        console.log('Inicializando sistema...\n');
        
        // Conectar ao banco de dados
        await connectDatabase();
        
        // Validar configurações
        if (!CONFIG.N8N_WEBHOOK_URL) {
            console.warn('⚠️ N8N_WEBHOOK_URL não configurada');
        }
        if (!CONFIG.EVOLUTION_API_URL) {
            console.warn('⚠️ EVOLUTION_API_URL não configurada');
        }
        
        console.log('\n✅ Sistema inicializado com sucesso!');
        console.log('============================================');
        console.log('CONFIGURAÇÕES:');
        console.log(`   N8N Webhook: ${CONFIG.N8N_WEBHOOK_URL}`);
        console.log(`   Evolution API: ${CONFIG.EVOLUTION_API_URL}`);
        console.log(`   PIX Timeout: ${Math.round(CONFIG.PIX_TIMEOUT/60000)} minutos`);
        console.log(`   Timezone: ${CONFIG.TIMEZONE}`);
        console.log('============================================\n');
        
    } catch (error) {
        console.error(`❌ Erro crítico na inicialização: ${error.message}`);
        process.exit(1);
    }
}

// ============================================
// TRATAMENTO DE ERROS E SINAIS
// ============================================

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection:', reason);
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    process.exit(1);
});

process.on('SIGINT', () => {
    console.log('\n🔄 Finalizando sistema...');
    if (database) database.end();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n🔄 Finalizando sistema...');
    if (database) database.end();
    process.exit(0);
});

// ============================================
// INICIAR SERVIDOR
// ============================================

initializeSystem().then(() => {
    app.listen(PORT, () => {
        console.log('============================================');
        console.log('🚀 SERVIDOR ONLINE');
        console.log('============================================');
        console.log(`   Porta: ${PORT}`);
        console.log(`   Dashboard: http://localhost:${PORT}`);
        console.log(`   Health: http://localhost:${PORT}/health`);
        console.log(`   Status: http://localhost:${PORT}/status`);
        console.log('============================================');
        console.log('WEBHOOKS:');
        console.log(`   Perfect Pay: http://localhost:${PORT}/webhook/perfect`);
        console.log(`   Evolution: http://localhost:${PORT}/webhook/evolution`);
        console.log(`   N8N Confirm: http://localhost:${PORT}/webhook/n8n-confirm`);
        console.log('============================================');
        console.log('DEBUG:');
        console.log(`   Conversa: http://localhost:${PORT}/debug/conversation/:phone`);
        console.log(`   Limpar flags: http://localhost:${PORT}/debug/clear-flags/:phone`);
        console.log('============================================');
        console.log('\n✅ SISTEMA v3.0 FUNCIONANDO!\n');
        console.log('Principais melhorias:');
        console.log('   ✅ Sistema de chaves 100% consistente');
        console.log('   ✅ Controle robusto de funil com bloqueio duplo');
        console.log('   ✅ Debug detalhado em todos os pontos');
        console.log('   ✅ Logs estruturados e informativos');
        console.log('   ✅ Endpoints de emergência para destravar');
        console.log('   ✅ Dashboard visual melhorado');
        console.log('   ✅ Limpeza automática de conversas antigas');
        console.log('============================================\n');
    });
    
    // Executar limpeza a cada 30 minutos
    setInterval(cleanupOldConversations, 30 * 60 * 1000);
    
    // Primeira limpeza após 1 minuto
    setTimeout(cleanupOldConversations, 60000);
});
